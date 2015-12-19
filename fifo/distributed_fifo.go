// the fifo struct
package fifo

import (
	"encoding/json"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
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
		fmt.Println("create the base znode")
		getZkConn().Create(path, data, int32(0), zk.WorldACL(zk.PermAll))
	} else {
		fmt.Println("the znode has exist")
	}
	return &fifo
}

//sequentially put a data into queue
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
	chidren, _, err := getZkConn().Children(this.basePath)
	return len(chidren), err
}

//get one data from znodes and delete the chosen znode
func (this *DistributedFIFO) Pop() (string, interface{}) {
	defer func() {
		e := recover()
		if e == zk.ErrConnectionClosed {
			//try reconnect the zk server
			fmt.Println("connection closed, reconnect to the zk server")
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
	return firstPath, data
}
