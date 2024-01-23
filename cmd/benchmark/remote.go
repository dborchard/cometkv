package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

func PrintIP() {
	resp, err := http.Get("https://icanhazip.com/")
	if err != nil {
		fmt.Println("Error fetching public IP:", err)
		return
	}
	defer resp.Body.Close()

	ip, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return
	}

	fmt.Println("EC2 Instance Public IP:", string(ip))
}
