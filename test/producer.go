package main

import (
	"github.com/nladuo/go-zk-fifo/fifo"
	"math/rand"
	"time"
)

const (
	hosts    []string = []string{"127.0.0.1:2181"}
	basePath string   = "fifo/"
)

var randSeed *rand.Rand

func produce(f *fifo.DistributedFIFO) {
	f.Push()
}

func main() {
	randSeed = rand.New(rand.NewSource(time.Now().UnixNano()))
	fifo.EstablishZkConn(hosts, basePath)
	myfifo := fifo.NewFifo()
	for i := 0; i < 10; i++ {
		go produce()
	}

	ch := make(chan int)
	<-ch
}
