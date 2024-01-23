package main

import (
	"fmt"
	"github.com/dborchard/cometkv/pkg/y/keygen"
	"math/rand"
	"time"
)

func main() {

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	keygen := keygen.Build(keygen.SEQUENTIAL, 0, 100)

	for i := 0; i < 100; i++ {
		key := keygen.Next(r)
		fmt.Println(key)
	}
}
