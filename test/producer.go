package main

import (
	"fmt"
	"github.com/nladuo/go-zk-fifo/fifo"
	"strconv"
	"time"
)

var (
	hosts    []string = []string{"127.0.0.1:2181"}
	basePath string   = "/fifo"
	fifoData []byte   = []byte("the fifo data")
	prefix   string   = "seq-"
)

func produce(f *fifo.DistributedFIFO) {
	for {
		size, err := f.Size()
		if err != nil {
			panic(err)
		}
		if size < 100 {
			data := "data---->" + strconv.FormatInt(time.Now().UnixNano(), 10)
			fmt.Println("Put : ", data)
			f.Put([]byte(data))
		}
	}
}

func main() {
	err := fifo.EstablishZkConn(hosts)
	if err != nil {
		panic(err)
	}
	myfifo := fifo.NewFifo(basePath, fifoData, prefix)
	for i := 0; i < 3; i++ {
		go produce(myfifo)
	}

	ch := make(chan int)
	<-ch
	fifo.CloseZkConn()
}
