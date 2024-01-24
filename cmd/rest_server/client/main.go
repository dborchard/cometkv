package main

import (
	"fmt"
)

func main() {
	c := NewClient()

	// write 2 entries
	c.Put("1", []byte("Alice"))
	c.Put("2", []byte("Bob"))
	rows := c.Scan("1", 2)
	for _, v := range rows {
		fmt.Println(string(v))
	}
	fmt.Println("-------")
	// update one entry
	c.Put("2", []byte("Bob2"))
	rows = c.Scan("1", 2)
	for _, v := range rows {
		fmt.Println(string(v))
	}
	fmt.Println("-------")

	// delete one entry
	c.Delete("1")
	rows = c.Scan("1", 2)
	for _, v := range rows {
		fmt.Println(string(v))
	}
	fmt.Println("-------")

	// get entries
	fmt.Println(string(c.Get("1")))
	fmt.Println(string(c.Get("2")))
	fmt.Println(string(c.Get("3")))
	fmt.Println("-------")

	c.Close()
}
