// the fifo struct
// Note: the one fifo's max index cannot exceed the range of int32
package fifo

import (
	"github.com/samuel/go-zookeeper/zk"
	"log"
)

type DistributedFIFO struct {
	prefix   string //the prefix of the distributed set znode
	basePath string
}

// create the fifo
func NewFifo(path string, data []byte, prefix string) *DistributedFIFO {
	var fifo DistributedFIFO
	fifo.prefix = prefix
	fifo.basePath = path
	isExsit, _, err := getZkConn().Exists(path)
	if err != nil {
		panic(err.Error())
	}
	if !isExsit {
		log.Println("create the znode:" + path)
		getZkConn().Create(path, data, int32(0), zk.WorldACL(zk.PermAll))
	} else {
		log.Println("the znode " + path + " existed")
	}
	return &fifo
}

//sequentially create a zonde
func (this *DistributedFIFO) Put(data []byte) {
	path := this.basePath + "/" + this.prefix
	getZkConn().Create(path, data, zk.FlagSequence, zk.WorldACL(zk.PermAll))
}

//get the size of the queue
func (this *DistributedFIFO) Size() (int, error) {
	chidren, _, err := getZkConn().Children(this.basePath)
	if err == zk.ErrConnectionClosed {
		//try reconnect the zk server
		log.Println("connection closed, reconnect to the zk server")
		reConnectZk()
	}
	return len(chidren), err
}

//get one data from znodes and delete the chosen znode
func (this *DistributedFIFO) Poll() (res []byte) {
	res = []byte{}
	defer func() {
		e := recover()
		if e == zk.ErrConnectionClosed {
			//try reconnect the zk server
			log.Println("connection closed, reconnect to the zk server")
			reConnectZk()
		}
	}()
REGET:
	chidren, _, err := getZkConn().Children(this.basePath)
	if err != nil {
		panic(err)
	}

	if len(chidren) == 0 {
		goto REGET
	}

	index := getMinIndex(chidren, this.prefix)
	firstPath := this.basePath + "/" + chidren[index] // for linux the file Seperator is /
	data, _, err := getZkConn().Get(firstPath)
	if err != nil {
		panic(err)
	}
	// delete the znode
	err = getZkConn().Delete(firstPath, 0)
	if err != nil {
		panic(err)
	}
	res = data
	return
}
