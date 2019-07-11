package zookeeper

import (
	"fmt"
	"github.com/camsiabor/qcom/qroutine"
	"github.com/camsiabor/qcom/util"
	"github.com/samuel/go-zookeeper/zk"
	"sync"
	"time"
)

type ZooWatcher struct {
	Id                string
	Endpoints         []string
	SessionTimeout    time.Duration
	ReconnectInterval time.Duration

	conn         *zk.Conn
	eventChannel <-chan zk.Event

	mutex sync.RWMutex
	timer *qroutine.Timer

	connected bool
}

func (o *ZooWatcher) Start(config map[string]interface{}) error {
	if o.IsConnected() {
		return fmt.Errorf("already started")
	}
	if o.Endpoints == nil || len(o.Endpoints) == 0 {
		o.Endpoints = util.GetStringSlice(config, "endpoints")
		if o.Endpoints == nil {
			o.Endpoints = []string{"127.0.0.1:2181"}
		}
	}

	if o.SessionTimeout <= 0 {
		var timeout = util.GetInt64("session.timeout", 10)
		o.SessionTimeout = time.Duration(timeout) * time.Second
	}

	var err error
	o.conn, o.eventChannel, err = zk.Connect(o.Endpoints, o.SessionTimeout)
	if err != nil {
		return err
	}
	o.connected = true

	return nil
}

func (o *ZooWatcher) Stop(map[string]interface{}) error {
	if !o.IsConnected() {
		return fmt.Errorf("not started yet")
	}
	if o.conn != nil {
		o.conn.Close()
		o.connected = false
	}
	if o.timer != nil {
		o.timer.Stop()
	}
	return nil
}

func (o *ZooWatcher) GetConn() *zk.Conn {
	return o.conn
}

func (o *ZooWatcher) IsConnected() bool {
	return o.connected
}
