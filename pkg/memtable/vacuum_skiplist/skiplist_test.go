package vacuum_skiplist

import (
	"context"
	memtable "github.com/dborchard/cometkv/pkg/memtable"
	tests "github.com/dborchard/cometkv/pkg/z"
	"testing"
	"time"
)

func Test1(t *testing.T) {
	tests.Test1(func(gcInterval, ttl time.Duration) memtable.IMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}

func Test2(t *testing.T) {
	tests.Test2(func(gcInterval, ttl time.Duration) memtable.IMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}

func Test3(t *testing.T) {
	tests.Test3(func(gcInterval, ttl time.Duration) memtable.IMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}

func Test4(t *testing.T) {
	tests.Test4(func(gcInterval, ttl time.Duration) memtable.IMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}

func Test5(t *testing.T) {
	tests.Test5(func(gcInterval, ttl time.Duration) memtable.IMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}

func Test6(t *testing.T) {
	tests.Test6(func(gcInterval, ttl time.Duration) memtable.IMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}

func Test7(t *testing.T) {
	tests.Test7(func(gcInterval, ttl time.Duration) memtable.IMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}

func Test8(t *testing.T) {
	tests.Test8(func(gcInterval, ttl time.Duration) memtable.IMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}

func Test9(t *testing.T) {
	tests.Test9(func(gcInterval, ttl time.Duration) memtable.IMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}

func Test10(t *testing.T) {
	tests.Test10(func(gcInterval, ttl time.Duration) memtable.IMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}

func Test11(t *testing.T) {
	tests.Test11(func(gcInterval, ttl time.Duration) memtable.IMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}
