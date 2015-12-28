//the zk initialization
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
	timeOut = 20
)

func getZkConn() *zk.Conn {
	return zkConn
}

func reConnectZk() {
	EstablishZkConn(hosts)
}

func EstablishZkConn(hosts []string) error {
	var err error
RECONNECT:
	zkConn, _, err = zk.Connect(hosts, timeOut*time.Second)
	if err != nil {
		goto RECONNECT
	}
	return err
}

func CloseZkConn() {
	zkConn.Close()
}
