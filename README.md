# TiKV GC Runner

**This program has not been tested**

The current TiKV clusters without TiDB can't do GC old MVCC data by themselves.
This simple program can run GC on TiKV cluster.

Do not run this with TiDB. Do not run multiple instances of this program on a single cluster.

## How to build

Make sure you have installed Go 1.12 and:

```bash
$ make build
```

## Parameters

###  `-pd` (string)

PD Addresses of the TiKV cluster, separated by commas. default: "127.0.0.1:2379".

### `-distributed` (bool)

Use distributed GC or not, default: true.

*Caution: Distributed GC is only supported since TiKV 3.0*

### `-concurrency` (int)

Concurrency of GC. Only useful when distributed is set to false. default: 2.

### `-run-interval` (Duration)

Interval to run GC. Default: 10m.

### `-life-time` (Duration)

GC life time. Default: 10m. Must not be less than 10m.

## Example

```bash
$ ./gc_runner -pd=192.168.1.101:2379,192.168.1.102:2379 -run-interval=30m
```
