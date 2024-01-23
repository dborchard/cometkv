package tests

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// Test1 STD test. (Using Delay)
// Writes: [ts-3, ts-2, ts-1, ts]
// Read: scan(l,r,ts), scan(l,r+1,ts)
func Test1(
	newTable func(gcInterval, ttl time.Duration) memtable.IMemtable,
	t *testing.T,
) {
	tbl := newTable(15*time.Second, 60*time.Second)

	tbl.Put("1", []byte("a")) // 10
	time.Sleep(1 * time.Second)
	tbl.Put("2", []byte("b")) // 11
	time.Sleep(1 * time.Second)
	tbl.Put("3", []byte("c")) // 12
	time.Sleep(1 * time.Second)
	tbl.Put("4", []byte("d")) // 13
	time.Sleep(1 * time.Second)

	assert.Equal(t, 4, tbl.Len())

	now := time.Now()
	rows := tbl.Scan("1", 1, now)
	assert.Equal(t, 1, len(rows))
	assert.Equal(t, []byte("a"), rows[0].Val)

	rows = tbl.Scan("1", 2, now)
	assert.Equal(t, 2, len(rows))
	assert.Equal(t, []byte("a"), rows[0].Val)
	assert.Equal(t, []byte("b"), rows[1].Val)

	rows = tbl.Scan("1", 3, now)
	assert.Equal(t, 3, len(rows))
	assert.Equal(t, []byte("a"), rows[0].Val)
	assert.Equal(t, []byte("b"), rows[1].Val)
	assert.Equal(t, []byte("c"), rows[2].Val)

	rows = tbl.Scan("1", 4, now)
	assert.Equal(t, 4, len(rows))
	assert.Equal(t, []byte("a"), rows[0].Val)
	assert.Equal(t, []byte("b"), rows[1].Val)
	assert.Equal(t, []byte("c"), rows[2].Val)
	assert.Equal(t, []byte("d"), rows[3].Val)

	tbl.Close()
}

// Test2 STD test. Using Delay, GC=2s for different segment
// Writes: [ts-3, ts-2] [ts-1, ts]
// Read: scan(l,r,ts), scan(l,r+1,ts)
func Test2(
	newTable func(gcInterval, ttl time.Duration) memtable.IMemtable,
	t *testing.T,
) {
	tbl := newTable(2*time.Second, 60*time.Second)

	tbl.Put("1", []byte("a")) // 10
	time.Sleep(1 * time.Second)
	tbl.Put("2", []byte("b")) // 11
	time.Sleep(1 * time.Second)
	tbl.Put("3", []byte("c")) // 12
	time.Sleep(1 * time.Second)
	tbl.Put("4", []byte("d")) // 13
	time.Sleep(1 * time.Second)

	assert.Equal(t, 4, tbl.Len())

	now := time.Now()
	rows := tbl.Scan("1", 1, now)
	assert.Equal(t, 1, len(rows))
	assert.Equal(t, []byte("a"), rows[0].Val)

	rows = tbl.Scan("1", 2, now)
	assert.Equal(t, 2, len(rows))
	assert.Equal(t, []byte("a"), rows[0].Val)
	assert.Equal(t, []byte("b"), rows[1].Val)

	rows = tbl.Scan("1", 3, now)
	assert.Equal(t, 3, len(rows))
	assert.Equal(t, []byte("a"), rows[0].Val)
	assert.Equal(t, []byte("b"), rows[1].Val)
	assert.Equal(t, []byte("c"), rows[2].Val)

	rows = tbl.Scan("1", 4, now)
	assert.Equal(t, 4, len(rows))
	assert.Equal(t, []byte("a"), rows[0].Val)
	assert.Equal(t, []byte("b"), rows[1].Val)
	assert.Equal(t, []byte("c"), rows[2].Val)
	assert.Equal(t, []byte("d"), rows[3].Val)

	tbl.Close()
}

// Test3 STD test. Using Delay, GC=1s for all different segment
// Writes: [ts-3], [ts-2], [ts-1], [ts]
// Read: scan(l,r,ts), scan(l,r+1,ts)
func Test3(
	newTable func(gcInterval, ttl time.Duration) memtable.IMemtable,
	t *testing.T,
) {
	tbl := newTable(1*time.Second, 60*time.Second)

	tbl.Put("1", []byte("a")) // 10
	time.Sleep(1 * time.Second)
	tbl.Put("2", []byte("b")) // 11
	time.Sleep(1 * time.Second)
	tbl.Put("3", []byte("c")) // 12
	time.Sleep(1 * time.Second)
	tbl.Put("4", []byte("d")) // 13
	time.Sleep(1 * time.Second)

	assert.Equal(t, 4, tbl.Len())

	now := time.Now()
	rows := tbl.Scan("1", 1, now)
	assert.Equal(t, 1, len(rows))
	assert.Equal(t, []byte("a"), rows[0].Val)

	rows = tbl.Scan("1", 2, now)
	assert.Equal(t, 2, len(rows))
	assert.Equal(t, []byte("a"), rows[0].Val)
	assert.Equal(t, []byte("b"), rows[1].Val)

	rows = tbl.Scan("1", 3, now)
	assert.Equal(t, 3, len(rows))
	assert.Equal(t, []byte("a"), rows[0].Val)
	assert.Equal(t, []byte("b"), rows[1].Val)
	assert.Equal(t, []byte("c"), rows[2].Val)

	rows = tbl.Scan("1", 4, now)
	assert.Equal(t, 4, len(rows))
	assert.Equal(t, []byte("a"), rows[0].Val)
	assert.Equal(t, []byte("b"), rows[1].Val)
	assert.Equal(t, []byte("c"), rows[2].Val)
	assert.Equal(t, []byte("d"), rows[3].Val)

	tbl.Close()
}

// Test4 STD test. Using Delay, GC=1s for all different segment, with Pruning, with expiredTime error
// Writes: [ts-3], [ts-2], [ts-1], [ts]
// Read: scan(l,r,ts), scan(l,r+1,ts)
func Test4(
	newTable func(gcInterval, ttl time.Duration) memtable.IMemtable,
	t *testing.T,
) {
	tbl := newTable(2*time.Second, 6*time.Second)

	for i := 1; i <= 10; i++ {
		key := fmt.Sprint(i)
		val := string(rune('a' + i - 1))
		tbl.Put(key, []byte(val))
		time.Sleep(time.Second)
	}

	now := time.Now()
	rows := tbl.Scan("1", 10, now)
	assert.True(t, len(rows) <= 7)

	snapTs := now.Add(-7 * time.Second)
	rows = tbl.Scan("1", 10, snapTs)
	assert.Equal(t, 0, len(rows))

	tbl.Close()
}

// Test5 Delete API.
func Test5(
	newTable func(gcInterval, ttl time.Duration) memtable.IMemtable,
	t *testing.T,
) {
	tbl := newTable(15*time.Second, 60*time.Second)

	tbl.Put("1", []byte("a"))
	tbl.Put("2", []byte("b"))
	tbl.Put("3", []byte("c"))

	rows := tbl.Scan("1", 2, time.Now())
	assert.Equal(t, 2, len(rows))
	assert.Equal(t, []byte("a"), rows[0].Val)
	assert.Equal(t, []byte("b"), rows[1].Val)

	rows = tbl.Scan("1", 3, time.Now())
	assert.Equal(t, 3, len(rows))
	assert.Equal(t, []byte("a"), rows[0].Val)
	assert.Equal(t, []byte("b"), rows[1].Val)
	assert.Equal(t, []byte("c"), rows[2].Val)

	tbl.Put("2", []byte("d"))
	rows = tbl.Scan("1", 3, time.Now())
	assert.Equal(t, 3, len(rows))
	assert.Equal(t, []byte("a"), rows[0].Val)
	assert.Equal(t, []byte("d"), rows[1].Val)
	assert.Equal(t, []byte("c"), rows[2].Val)

	tbl.Delete("1")
	rows = tbl.Scan("1", 3, time.Now())
	assert.Equal(t, 2, len(rows))
	assert.Equal(t, []byte("d"), rows[0].Val)
	assert.Equal(t, []byte("c"), rows[1].Val)

	// get entries
	assert.Equal(t, []byte{}, tbl.Get("1", time.Now()))
	assert.Equal(t, []byte("d"), tbl.Get("2", time.Now()))
	assert.Equal(t, []byte("c"), tbl.Get("3", time.Now()))

	tbl.Close()
}

// Test8 Update same key at different time. Verify Get()
func Test8(
	newTable func(gcInterval, ttl time.Duration) memtable.IMemtable,
	t *testing.T,
) {
	tbl := newTable(15*time.Second, 60*time.Second)

	tbl.Put("1", []byte("a")) // 10
	time.Sleep(1 * time.Second)
	tbl.Put("1", []byte("b")) // 11
	time.Sleep(1 * time.Second)
	tbl.Put("1", []byte("c")) // 12
	time.Sleep(1 * time.Second)
	tbl.Put("1", []byte("d")) // 13
	time.Sleep(1 * time.Second)

	assert.Equal(t, 4, tbl.Len())

	assert.Equal(t, []byte("a"), tbl.Get("1", time.Now().Add(-4*time.Second)))
	assert.Equal(t, []byte("b"), tbl.Get("1", time.Now().Add(-3*time.Second)))
	assert.Equal(t, []byte("c"), tbl.Get("1", time.Now().Add(-2*time.Second)))
	assert.Equal(t, []byte("d"), tbl.Get("1", time.Now().Add(-1*time.Second)))

	tbl.Close()
}

// Test9 Update same key at different time. Verify Scan
func Test9(
	newTable func(gcInterval, ttl time.Duration) memtable.IMemtable,
	t *testing.T,
) {
	tbl := newTable(15*time.Second, 60*time.Second)

	tbl.Put("1", []byte("a")) // 10
	time.Sleep(1 * time.Second)
	tbl.Put("1", []byte("b")) // 11
	time.Sleep(1 * time.Second)
	tbl.Put("1", []byte("c")) // 12
	time.Sleep(1 * time.Second)
	tbl.Put("1", []byte("d")) // 13
	time.Sleep(1 * time.Second)

	assert.Equal(t, 4, tbl.Len())

	rows := tbl.Scan("1", 2, time.Now().Add(-4*time.Second))
	assert.Equal(t, 1, len(rows))
	assert.Equal(t, []byte("a"), rows[0].Val)

	rows = tbl.Scan("1", 2, time.Now().Add(-3*time.Second))
	assert.Equal(t, 1, len(rows))
	assert.Equal(t, []byte("b"), rows[0].Val)

	rows = tbl.Scan("1", 2, time.Now().Add(-2*time.Second))
	assert.Equal(t, 1, len(rows))
	assert.Equal(t, []byte("c"), rows[0].Val)

	rows = tbl.Scan("1", 2, time.Now().Add(-1*time.Second))
	assert.Equal(t, 1, len(rows))
	assert.Equal(t, []byte("d"), rows[0].Val)

	tbl.Close()
}

// Test10 Scan with Snapshot Time.
func Test10(
	newTable func(gcInterval, ttl time.Duration) memtable.IMemtable,
	t *testing.T,
) {
	tbl := newTable(15*time.Second, 60*time.Second)

	tbl.Put("1", []byte("a")) // 10
	time.Sleep(1 * time.Second)
	tbl.Put("2", []byte("b")) // 11
	time.Sleep(1 * time.Second)
	tbl.Put("3", []byte("c")) // 12
	time.Sleep(1 * time.Second)
	tbl.Put("4", []byte("d")) // 13
	time.Sleep(1 * time.Second)

	assert.Equal(t, 4, tbl.Len())

	rows := tbl.Scan("2", 4, time.Now().Add(-2*time.Second))
	assert.Equal(t, 2, len(rows))
	assert.Equal(t, []byte("b"), rows[0].Val)
	assert.Equal(t, []byte("c"), rows[1].Val)

	rows = tbl.Scan("1", 4, time.Now().Add(-2*time.Second))
	assert.Equal(t, 3, len(rows))
	assert.Equal(t, []byte("a"), rows[0].Val)
	assert.Equal(t, []byte("b"), rows[1].Val)
	assert.Equal(t, []byte("c"), rows[2].Val)

	rows = tbl.Scan("1", 4, time.Now().Add(-1*time.Second))
	assert.Equal(t, 4, len(rows))
	assert.Equal(t, []byte("a"), rows[0].Val)
	assert.Equal(t, []byte("b"), rows[1].Val)
	assert.Equal(t, []byte("c"), rows[2].Val)
	assert.Equal(t, []byte("d"), rows[3].Val)

	tbl.Close()
}

// Test11 Scan records from beginning
func Test11(
	newTable func(gcInterval, ttl time.Duration) memtable.IMemtable,
	t *testing.T,
) {
	tbl := newTable(15*time.Second, 60*time.Second)

	tbl.Put("1", []byte("a")) // 10
	time.Sleep(1 * time.Second)
	tbl.Put("2", []byte("b")) // 11
	time.Sleep(1 * time.Second)
	tbl.Put("3", []byte("c")) // 12
	time.Sleep(1 * time.Second)
	tbl.Put("4", []byte("d")) // 13
	time.Sleep(1 * time.Second)

	assert.Equal(t, 4, tbl.Len())

	rows := tbl.Scan("", tbl.Len(), time.Now())
	assert.Equal(t, 4, len(rows))
	assert.Equal(t, []byte("a"), rows[0].Val)
	assert.Equal(t, []byte("b"), rows[1].Val)
	assert.Equal(t, []byte("c"), rows[2].Val)
	assert.Equal(t, []byte("d"), rows[3].Val)

	tbl.Close()
}
