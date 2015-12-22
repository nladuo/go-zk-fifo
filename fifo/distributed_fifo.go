// the fifo struct
package fifo

import (
	"encoding/json"
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"sync"
)

type DistributedFIFO struct {
	prefix   string //the prefix of the distributed set znode
	basePath string
	lock     *sync.Mutex
}

// create the fifo
func NewFifo(path string, data []byte, prefix string) *DistributedFIFO {
	var fifo DistributedFIFO
	fifo.lock = new(sync.Mutex)
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
func (this *DistributedFIFO) Push(data interface{}) {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	path := this.basePath + "/" + this.prefix
	getZkConn().Create(path, dataBytes, zk.FlagSequence, zk.WorldACL(zk.PermAll))
}

//get the size of the queue
func (this *DistributedFIFO) Size() (int, error) {
	this.lock.Lock()
	chidren, _, err := getZkConn().Children(this.basePath)
	this.lock.Unlock()
	return len(chidren), err
}

//get one data from znodes and delete the chosen znode
func (this *DistributedFIFO) Pop() interface{} {
	this.lock.Lock()
	defer func() {
		e := recover()
		if e == zk.ErrConnectionClosed {
			//try reconnect the zk server
			log.Println("connection closed, reconnect to the zk server")
			reConnectZk()
		}
		if (e != nil) && (e != zk.ErrNoNode) {
			panic(e)
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
	dataBytes, _, err := getZkConn().Get(firstPath)
	if err != nil {
		panic(err)
	}
	var data interface{}
	//unserialize the data
	err = json.Unmarshal(dataBytes, &data)
	if err != nil {
		panic(err)
	}
	// delete the znode
	err = getZkConn().Delete(firstPath, 0)
	if err != nil {
		panic(err)
	}
	this.lock.Unlock()
	return data
}
