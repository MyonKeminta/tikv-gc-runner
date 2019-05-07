package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/gcworker"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var (
	pdServers   = flag.String("pd", "127.0.0.1:2379", "PD Addresses of the TiKV cluster. default: 127.0.0.1:2379")
	distributed = flag.Bool("distributed", true, "Use distributed GC or not, default: true. Use false for versions 2.x")
	concurrency = flag.Int("concurrency", 2, "Concurrency of GC. Only useful when distributed is set to false. default: 2")

	runInterval = flag.Duration("run-interval", time.Minute*10, "Interval to run GC. Default: 10m")
	gcLifeTime  = flag.Duration("life-time", time.Minute*10, "GC life time. Default: 10m. Must not less than 10m")
)

func createClient() (store kv.Storage, pdClient pd.Client) {
	driver := tikv.Driver{}
	var err error
	store, err = driver.Open(fmt.Sprintf("tikv://%s?disableGC=true", *pdServers))
	if err != nil {
		log.Fatalf("Failed to open db: %v", err)
	}

	addresses := strings.Split(*pdServers, ",")
	pdClient, err = pd.NewClient(addresses, pd.SecurityOption{})
	if err != nil {
		log.Fatalf("Failed to create pdClient: %v", err)
	}

	return
}

type gcRunner struct {
	store kv.Storage
	pd    pd.Client

	isRunning bool
	lastRun   time.Time

	finishCh chan interface{}
}

func newGCRunner(store kv.Storage, pd pd.Client) *gcRunner {
	return &gcRunner{
		store:     store,
		pd:        pd,
		isRunning: false,
		finishCh:  make(chan interface{}, 1),
	}
}

func (r *gcRunner) RunGCLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.tick(ctx)
		case <-r.finishCh:
			r.isRunning = false
		}
	}
}

func (r *gcRunner) tick(ctx context.Context) {
	log.Printf("tick")

	if r.isRunning {
		log.Printf("tick: GC is running. Skip.")
		return
	}

	ver, err := r.store.CurrentVersion()
	if err != nil {
		log.Printf("[ERR] Cannot get tso: %v", err)
	}

	physical := oracle.ExtractPhysical(ver.Ver)
	now := time.Unix(physical/1e3, (physical%1e3)*1e6)
	safePointTime := now.Add(-*gcLifeTime)
	safePoint := oracle.ComposeTS(oracle.GetPhysical(safePointTime), 0)

	if time.Since(r.lastRun) < *runInterval {
		log.Printf("tick: GC run interval hasn't past. Skip.")
		return
	}

	log.Printf("Start GC at time: %v, safePoint: %v (%v)", now, safePoint, safePointTime)

	r.isRunning = true
	go func() {
		r.RunGC(ctx, safePoint)
		r.finishCh <- nil
	}()
}

func (r *gcRunner) RunGC(ctx context.Context, safePoint uint64) {
	gcWorkerIdentifier := fmt.Sprintf("gcworker-%v", safePoint)

	var err error
	if *distributed {
		err = gcworker.RunDistributedGCJob(ctx, r.store.(tikv.Storage), r.pd, safePoint, gcWorkerIdentifier)
	} else {
		err = gcworker.RunGCJob(ctx, r.store.(tikv.Storage), safePoint, gcWorkerIdentifier, *concurrency)
	}
	if err != nil {
		fmt.Printf("[ERR] GC job failed: %v", err)
	}
}

func main() {
	flag.Parse()

	if *gcLifeTime < time.Minute*10 {
		log.Fatalf("life-time can't be set below 10 minutes")
	}

	log.Printf("GC Runner started")
	log.Printf("PD Servers: %v", *pdServers)
	log.Printf("Distributed: %v", *distributed)
	log.Printf("Concurrency: %v", *concurrency)
	log.Printf("Run Interval: %v", *runInterval)
	log.Printf("GC Life Time: %v", *gcLifeTime)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, pdClient := createClient()
	defer func() {
		pdClient.Close()
		err := store.Close()
		if err != nil {
			log.Fatalf("Failed to close store: %v", err)
		}
	}()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		sig := <-sc
		log.Printf("Received signal [%s]. Exit.", sig)
		cancel()
		os.Exit(0)
	}()

	newGCRunner(store, pdClient).RunGCLoop(ctx)
}
