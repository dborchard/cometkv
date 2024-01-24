package main

import (
	"context"
	"fmt"
	"github.com/dborchard/cometkv/pkg/kv"
	"github.com/dborchard/cometkv/pkg/memtable"
	"github.com/dborchard/cometkv/pkg/sst"
	common "github.com/dborchard/cometkv/pkg/y/entry"
	"github.com/gin-gonic/gin"
	"io"
	"net/http"
	"strconv"
	"time"
	"unsafe"
)

func main() {
	memTableType := memtable.HWTBTree
	gcInterval := 30 * time.Second   // 5sec, 30sec, 1m
	ttl := 3 * time.Minute           // 3min
	flushInterval := 1 * time.Minute // 1min

	kvStore := kv.NewCometKV(context.Background(), memTableType, sst.MBtree, gcInterval, ttl, flushInterval)

	r := gin.New()
	r.Use(gin.Recovery())
	r.POST("/put/:key", func(c *gin.Context) {
		key := c.Param("key")
		byteBody, _ := io.ReadAll(c.Request.Body)
		kvStore.Put(key, byteBody)
		c.Data(http.StatusOK, "application/octet-stream", nil)
	})

	r.GET("/get/:key", func(c *gin.Context) {
		key := c.Param("key")
		byteBody := kvStore.Get(key, time.Now())
		c.Data(http.StatusOK, "application/octet-stream", byteBody)
	})

	r.GET("/scan/:key/:count", func(c *gin.Context) {
		key := c.Param("key")
		count, _ := strconv.Atoi(c.Param("count"))
		items := kvStore.Scan(key, count, time.Now())
		output := ListToByteArray(items, count)
		c.Data(http.StatusOK, "application/octet-stream", output)
	})

	r.DELETE("/delete/:key", func(c *gin.Context) {
		key := c.Param("key")
		kvStore.Delete(key)
		c.Data(http.StatusOK, "application/octet-stream", nil)
	})

	fmt.Println("Started Server with", memTableType)
	err := r.Run()
	if err != nil {
		panic(err)
	}
}

func ListToByteArray(items []common.Pair[string, []byte], count int) []byte {
	var output []byte

	if len(items) > count {
		panic("len(output) > count")
	}

	for _, item := range items {
		val := item.Val

		vLen := int64(len(val))
		vLenBytes := unsafe.Slice((*byte)(unsafe.Pointer(&vLen)), 8)

		output = append(output, vLenBytes...)
		output = append(output, val...)
	}

	return output
}
