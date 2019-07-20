package etcd

import (
	"fmt"
	"github.com/camsiabor/go-zookeeper/zk"
	"github.com/camsiabor/qcom/qroutine"
	"github.com/camsiabor/qcom/util"
	"go.etcd.io/etcd/clientv3"
	"golang.org/x/net/context"
	"log"
	"sync"
	"time"
)

type EtcdConnectCallback func(event *zk.Event, watcher *EtcdWatcher, err error)

type EtcdWatcher struct {
	Id            string
	Endpoints     []string
	HeartbeatPath string

	SessionTimeout    time.Duration
	ReconnectInterval time.Duration

	Logger *log.Logger

	connectCallbacks []EtcdConnectCallback

	conn  *clientv3.Client
	lease clientv3.Lease

	mutex sync.RWMutex

	watchMutex sync.RWMutex
	watches    map[string]*WatchBox

	connected           bool
	connectChannel      []chan bool
	connectChannelMutex sync.Mutex

	reconnectTimer *qroutine.Timer
}

func (o *EtcdWatcher) Start(config map[string]interface{}) error {

	o.mutex.Lock()
	defer o.mutex.Unlock()

	if o.watches == nil {
		o.watches = map[string]*WatchBox{}
	}

	if o.Endpoints == nil || len(o.Endpoints) == 0 {
		o.Endpoints = util.GetStringSlice(config, "endpoints")
		if o.Endpoints == nil {
			o.Endpoints = []string{"127.0.0.1:2379"}
		}
	}

	if o.SessionTimeout <= 0 {
		var timeout = util.GetInt64(config, 7, "session.timeout")
		o.SessionTimeout = time.Duration(timeout) * time.Second
	}

	if o.ReconnectInterval <= 0 {
		var interval = util.GetInt64(config, 10, "reconnect.interval")
		o.ReconnectInterval = time.Duration(interval) * time.Second
	}

	if o.connected {
		return fmt.Errorf("already connected")
	}
	if o.reconnectTimer == nil {
		o.reconnectTimer = &qroutine.Timer{}
		return o.reconnectTimer.Start(0, o.ReconnectInterval, o.connectLoop)
	}

	return nil
}

func (o *EtcdWatcher) connectLoop(timer *qroutine.Timer, err error) {

	if err != nil {
		o.Logger.Println("etcd watcher reconnect error", err.Error())
		return
	}

	if o.conn == nil {
		func() {
			o.conn, err = clientv3.New(clientv3.Config{
				Endpoints:   o.Endpoints,
				DialTimeout: o.SessionTimeout,
			})

			if err == nil {
				o.connected = true
				o.notifyConnected()
			}
		}()
	}

	if o.lease == nil {
		o.lease = clientv3.NewLease(o.conn)
	}

	var ctx, _ = context.WithTimeout(context.TODO(), o.ReconnectInterval)
	grant, err := o.lease.Grant(ctx, int64(o.ReconnectInterval/time.Second)+2)
	if err != nil {
		o.connected = false
		o.Logger.Println(err)
		return
	}

	ctx, _ = context.WithTimeout(context.TODO(), o.ReconnectInterval)
	put, err := o.conn.Put(ctx, o.HeartbeatPath, "")
	fmt.Println(grant.ID)

}

func (o *EtcdWatcher) Stop(map[string]interface{}) error {

	o.mutex.Lock()
	defer o.mutex.Unlock()

	o.notifyConnected(false)

	if o.reconnectTimer != nil {
		o.reconnectTimer.Stop()
	}

	if o.conn == nil {
		return fmt.Errorf("not connected yet")
	}

	if o.conn != nil {
		o.conn.Close()
		o.connected = false
	}

	return nil
}

func (o *EtcdWatcher) AddConnectCallback(callback EtcdConnectCallback) {
	if callback == nil {
		panic("null connect callback")
	}
	o.mutex.Lock()
	defer o.mutex.Unlock()

	if o.connectCallbacks == nil {
		o.connectCallbacks = []EtcdConnectCallback{callback}
	} else {
		o.connectCallbacks = append(o.connectCallbacks, callback)
	}
}

func (o *EtcdWatcher) getWatch(wtype WatchType, path string, lock bool) *WatchBox {
	if lock {
		o.watchMutex.RLock()
		defer o.watchMutex.RUnlock()
	}
	return o.watches[path]
}

func (o *EtcdWatcher) Watch(wtype WatchType, path string, data interface{}, routine WatchRoutine) {
	var box = o.getWatch(wtype, path, true)
	if box != nil {
		box.routine = routine
		return
	}
	o.watchMutex.Lock()
	defer o.watchMutex.Unlock()

	box = o.getWatch(wtype, path, false)
	if box != nil {
		return
	}

	box = &WatchBox{}
	box.wtype = wtype
	box.Path = path
	box.routine = routine
	box.control = make(chan bool, 2)
	box.watcher = o
	box.Data = data

	o.watches[path] = box

	box.Logger = o.Logger

	_ = o.conn.Watch(context.Background(), path)

	go box.loop()
}

func (o *EtcdWatcher) Create(path string, data []byte, createflags int32) (bool, error) {

	get, err := o.conn.Get(context.TODO(), path)
	if err != nil {
		return false, err
	}

	if get.Kvs != nil {
		return true, nil
	}

	_, err = o.conn.Put(context.TODO(), path, string(data))
	return false, err
}

func (o *EtcdWatcher) GetConn() *clientv3.Client {
	return o.conn
}

func (o *EtcdWatcher) IsConnected() bool {
	return o.connected
}

func (o *EtcdWatcher) notifyConnected(value bool) {
	if o.connectChannel == nil {
		return
	}
	o.connectChannelMutex.Lock()
	defer o.connectChannelMutex.Unlock()
	if o.connectChannel == nil {
		return
	}
	for _, ch := range o.connectChannel {
		ch <- value
		close(ch)
	}
	o.connectChannel = nil
}

func (o *EtcdWatcher) WaitForConnected() <-chan bool {
	if o.IsConnected() {
		return nil
	}
	var ch = make(chan bool)
	o.connectChannelMutex.Lock()
	defer o.connectChannelMutex.Unlock()
	if o.connectChannel == nil {
		o.connectChannel = []chan bool{ch}
	} else {
		o.connectChannel = append(o.connectChannel, ch)
	}
	return ch
}
