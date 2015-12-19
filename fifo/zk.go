package fifo

import (
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

var (
	zkConn *zk.Conn
	hosts  []string
)

const (
	timeOut = 6
)

func GetZkConn() *zk.Conn {
	return zkConn
}

func ReConnectZk() {
	EstablishZkConn(hosts)
}

func EstablishZkConn(hosts []string) error {
	zkConn, _, err := zk.Connect(hosts, timeOut*time.Second)
	return err
}

func CloseZkConn() {
	zkConn.Close()
}
