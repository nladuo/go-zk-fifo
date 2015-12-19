package fifo

import (
	"encoding/json"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
)

type DistributedFIFO struct {
	prefix   string //the prefix of the distributed set znode
	basePath string
	conn     *zk.Conn
}

// create the fifo
func NewFifo(path string, data []byte, prefix string) *DistributedFIFO {
	var fifo DistributedFIFO
	fifo.prefix = prefix
	fifo.basePath = path
	fifo.conn = GetZkConn()
	isExsit, _, err := fifo.conn.Exists(path)
	if err != nil {
		panic(err.Error())
	}
	if !isExsit {
		fifo.conn.Create(path, data, int32(0), zk.WorldACL(zk.PermAll))
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
	this.conn.Create(path, dataBytes, zk.FlagSequence, zk.WorldACL(zk.PermAll))
}

//get the size of the queue
func (this *DistributedFIFO) Size() (int, error) {
	chidren, _, err := this.conn.Children(this.basePath)
	return len(chidren), err
}

//get one data from znodes and delete the chosen znode
func (this *DistributedFIFO) Pop() interface{} {
	defer func() {
		e := recover()
		if e == zk.ErrConnectionClosed {
			//try reconnect the zk server
			fmt.Println("connection closed, reconnect to the zk server")
			ReConnectZk()
			this.conn = GetZkConn()
			//EstablishZkConn(hosts)
		}
		if (e != nil) && (e != zk.ErrNoNode) {
			panic(e)
		}
	}()
REGET:
	chidren, _, err := this.conn.Children(this.basePath)
	if err != nil {
		panic(err)
	}

	if len(chidren) == 0 {
		goto REGET
	}
	index := GetMinIndex(chidren, this.prefix)
	firstPath := this.basePath + "/" + chidren[index] // for linux the file Seperator is /
	dataBytes, _, err := this.conn.Get(firstPath)
	if err != nil {
		panic(err)
	}
	var data interface{}
	//unserialize the data
	err = json.Unmarshal(dataBytes, &data)
	if err != nil {
		panic(err)
	}
	err = this.conn.Delete(firstPath, 0)
	if err != nil {
		panic(err)
	}
	return data
}
