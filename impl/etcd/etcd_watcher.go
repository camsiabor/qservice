package etcd

import (
	"fmt"
	"github.com/camsiabor/qcom/qroutine"
	"github.com/camsiabor/qcom/util"
	"go.etcd.io/etcd/clientv3"
	"golang.org/x/net/context"
	"log"
	"sync"
	"time"
)

type EtcdWatcherEvent int

const (
	EtcdWatcherEventConnected    EtcdWatcherEvent = 1
	EtcdWatcherEventDisconnected EtcdWatcherEvent = 2
)

type EtcdWatcherCallback func(event EtcdWatcherEvent, watcher *EtcdWatcher, err error)

type EtcdWatcher struct {
	Id            string
	Endpoints     []string
	HeartbeatPath string

	SessionTimeout    time.Duration
	ReconnectInterval time.Duration

	Logger *log.Logger

	callbacks []EtcdWatcherCallback

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
				o.notifyConnectStateChange(true, true)
			}
		}()
	}

	if o.lease == nil {
		o.lease = clientv3.NewLease(o.conn)
	}

	var ctx, _ = context.WithTimeout(context.TODO(), o.ReconnectInterval)
	grant, err := o.lease.Grant(ctx, int64(o.ReconnectInterval/time.Second)+2)
	if err != nil {
		o.Logger.Println(err)
		o.notifyConnectStateChange(false, false)
		return
	}

	ctx, _ = context.WithTimeout(context.TODO(), o.ReconnectInterval)
	_, err = o.conn.Put(ctx, o.HeartbeatPath, "", clientv3.WithLease(grant.ID))
	if err != nil {
		o.notifyConnectStateChange(false, false)
		return
	}

	o.notifyConnectStateChange(false, true)

}

func (o *EtcdWatcher) Stop(map[string]interface{}) error {

	o.mutex.Lock()
	defer o.mutex.Unlock()

	if o.reconnectTimer != nil {
		o.reconnectTimer.Stop()
	}

	o.notifyConnectStateChange(false)

	if o.conn == nil {
		return fmt.Errorf("not connected yet")
	}

	if o.conn != nil {
		_ = o.conn.Close()
		o.connected = false
	}

	return nil
}

func (o *EtcdWatcher) AddConnectCallback(callback EtcdWatcherCallback) {
	if callback == nil {
		panic("null connect callback")
	}
	o.mutex.Lock()
	defer o.mutex.Unlock()

	if o.callbacks == nil {
		o.callbacks = []EtcdWatcherCallback{callback}
	} else {
		o.callbacks = append(o.callbacks, callback)
	}
}

func (o *EtcdWatcher) getWatch(wtype WatchType, path string, lock bool) *WatchBox {
	if lock {
		o.watchMutex.RLock()
		defer o.watchMutex.RUnlock()
	}
	return o.watches[path]
}

func (o *EtcdWatcher) Watch2(wtype WatchType, path string, data interface{}, routine WatchRoutine) {
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

func (o *EtcdWatcher) GetContextWithTimeout(parent context.Context, timeout time.Duration) context.Context {
	var ctx context.Context
	if timeout <= 0 {
		ctx = context.TODO()
	} else {
		if parent == nil {
			parent = context.TODO()
		}
		ctx, _ = context.WithTimeout(parent, timeout)
	}
	return ctx
}

func (o *EtcdWatcher) Get(path string, timeout time.Duration, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if !o.connected {
		return nil, fmt.Errorf("disconnected")
	}
	var ctx = o.GetContextWithTimeout(nil, timeout)
	return o.conn.Get(ctx, path, opts...)
}

func (o *EtcdWatcher) Put(path string, val string, timeout time.Duration, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	if !o.connected {
		return nil, fmt.Errorf("disconnected")
	}
	var ctx = o.GetContextWithTimeout(nil, timeout)
	return o.conn.Put(ctx, path, val, opts...)
}

func (o *EtcdWatcher) PutWithTTL(path string, val string, ttl int64, timeout time.Duration, opts ...clientv3.OpOption) (*clientv3.PutResponse, *clientv3.LeaseGrantResponse, error) {
	if !o.connected {
		return nil, nil, fmt.Errorf("disconnected")
	}
	var ctx = o.GetContextWithTimeout(nil, timeout)
	var leaseresp, err = o.conn.Grant(ctx, ttl)
	if err != nil {
		return nil, nil, err
	}
	if opts == nil {
		opts = []clientv3.OpOption{clientv3.WithLease(leaseresp.ID)}
	} else {
		opts = append(opts, clientv3.WithLease(leaseresp.ID))
	}
	ctx = o.GetContextWithTimeout(nil, timeout)
	put, err := o.conn.Put(ctx, path, val, opts...)
	if err != nil {
		return nil, nil, err
	}
	return put, leaseresp, nil
}

func (o *EtcdWatcher) Delete(path string, timeout time.Duration, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	if !o.connected {
		return nil, fmt.Errorf("disconnected")
	}
	var ctx = o.GetContextWithTimeout(nil, timeout)
	return o.conn.Delete(ctx, path, opts...)
}

func (o *EtcdWatcher) Compact(recv int64, timeout time.Duration, opts ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	if !o.connected {
		return nil, fmt.Errorf("disconnected")
	}
	var ctx = o.GetContextWithTimeout(nil, timeout)
	return o.conn.Compact(ctx, recv, opts...)
}

func (o *EtcdWatcher) Watch(path string, timeout time.Duration, opts ...clientv3.OpOption) clientv3.WatchChan {
	if !o.connected {
		return nil
	}

	var ctx = o.GetContextWithTimeout(nil, timeout)
	return o.conn.Watch(ctx, path, opts...)
}

func (o *EtcdWatcher) Create(path string, data string, timeout time.Duration) (bool, error) {
	get, err := o.Get(path, timeout, nil)
	if err != nil {
		return false, err
	}
	if get.Kvs != nil {
		return true, nil
	}
	_, err = o.Put(path, data, timeout)
	return false, err
}

func (o *EtcdWatcher) GetConn() *clientv3.Client {
	return o.conn
}

func (o *EtcdWatcher) IsConnected() bool {
	return o.connected
}

func (o *EtcdWatcher) invokeCallbacks(event EtcdWatcherEvent) {
	if o.callbacks == nil {
		return
	}
	for _, callback := range o.callbacks {
		if callback != nil {
			go callback(EtcdWatcherEventConnected, o, nil)
		}
	}
}

func (o *EtcdWatcher) notifyConnectStateChange(value bool, connected bool) {

	o.connected = connected

	if o.connected {
		o.invokeCallbacks(EtcdWatcherEventConnected)
	} else {
		o.invokeCallbacks(EtcdWatcherEventDisconnected)
	}

	if !o.connected {
		return
	}

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
