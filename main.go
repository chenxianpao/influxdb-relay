package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/influxdb-relay/relay"
)

// "github.com/lafikl/consistent"
func main() {
	// c := consistent.New()
	// // adds the hosts to the ring
	// c.Add("127.0.0.1:8000")
	// c.Add("92.0.0.1:8000")
	// c.Add("93.0.0.1:8000")
	// c.Add("94.0.0.1:8000")
	// c.Add("95.0.0.1:8000")
	// c.Add("96.0.0.1:8000")
	// Returns the host that owns `key`.
	//
	// As described in https://en.wikipedia.org/wiki/Consistent_hashing
	//
	// It returns ErrNoHosts if the ring has no hosts in it.
	// host, err := c.Get("cpu_usage")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Println(host)

	// host1, err1 := c.Get("mem_usage")
	// if err1 != nil {
	// 	log.Fatal(err1)
	// }
	// fmt.Println(host1)
	cfg, err := relay.LoadConfigFile("/Users/chenxianpao/Go/src/github.com/influxdb-relay/config.toml")
	if err != nil {
		fmt.Fprintln(os.Stderr, "Problem loading config file:", err)
	}
	r, err := relay.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	go func() {
		<-sigChan
		r.Stop()
	}()

	log.Println("starting relays...")
	r.Run()
}
