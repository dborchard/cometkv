# Test Run Commands

## Benchmark
- Build
```shell
go build segment_ring
```


- CPU and Memory Profile
```shell
go test -run=XXX -bench=. -benchmem -memprofile mem.prof -cpuprofile cpu.prof -benchtime=30s
```

- Flame Graph
```shell
go tool pprof -http=":8000" pprofbin cpu.prof
```

- CLI interface
```shell
go tool pprof cpu.prof
```

## 