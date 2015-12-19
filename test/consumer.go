package main

import (
	"fmt"
	"github.com/nladuo/go-zk-fifo/fifo"
)

var (
	hosts    []string = []string{"127.0.0.1:2181"}
	basePath string   = "/fifo"
	fifoData []byte   = []byte("the fifo data")
	prefix   string   = "seq-"
)

func consume(f *fifo.DistributedFIFO) {
	for {
		//time.Sleep(2 * time.Second)
		data := f.Pop()
		if data != nil {
			fmt.Println("Pop : ", data)
		}
	}
}

func main() {
	fifo.EstablishZkConn(hosts)
	myfifo := fifo.NewFifo(basePath, fifoData, prefix)
	for i := 0; i < 10; i++ {
		go consume(myfifo)
	}

	ch := make(chan int)
	<-ch
	fifo.CloseZkConn()
}
