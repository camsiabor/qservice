package zookeeper

import (
	"fmt"
	"github.com/camsiabor/go-zookeeper/zk"
	"github.com/camsiabor/qcom/qroutine"
	"github.com/camsiabor/qcom/util"
	"sync"
	"time"
)

type ZooConnectCallback func(event *zk.Event, watcher *ZooWatcher, err error)

type ZooWatcher struct {
	Id                string
	Endpoints         []string
	SessionTimeout    time.Duration
	ReconnectInterval time.Duration
	connectCallbacks  []ZooConnectCallback

	conn         *zk.Conn
	eventChannel <-chan zk.Event

	mutex sync.RWMutex

	watchMutex    sync.RWMutex
	watchGet      map[string]*WatchBox
	watchExist    map[string]*WatchBox
	watchChildren map[string]*WatchBox

	connected           bool
	connectChannel      []chan bool
	connectChannelMutex sync.Mutex

	reconnectTimer *qroutine.Timer
}

func (o *ZooWatcher) Start(config map[string]interface{}) error {

	o.mutex.Lock()
	defer o.mutex.Unlock()

	if o.watchGet == nil {
		o.watchGet = map[string]*WatchBox{}
	}
	if o.watchExist == nil {
		o.watchExist = map[string]*WatchBox{}
	}
	if o.watchChildren == nil {
		o.watchChildren = map[string]*WatchBox{}
	}

	if o.Endpoints == nil || len(o.Endpoints) == 0 {
		o.Endpoints = util.GetStringSlice(config, "endpoints")
		if o.Endpoints == nil {
			o.Endpoints = []string{"127.0.0.1:2181"}
		}
	}

	if o.SessionTimeout <= 0 {
		var timeout = util.GetInt64(config, 10, "session.timeout")
		o.SessionTimeout = time.Duration(timeout) * time.Second
	}

	if o.ReconnectInterval <= 0 {
		var interval = util.GetInt64(config, 10, "reconnect.interval")
		o.ReconnectInterval = time.Duration(interval) * time.Second
	}

	if o.conn != nil {
		return fmt.Errorf("already connected")
	}
	o.reconnectTimer = &qroutine.Timer{}
	return o.reconnectTimer.Start(0, o.ReconnectInterval, o.reconnect)

}

func (o *ZooWatcher) reconnect(timer *qroutine.Timer, err error) {
	o.conn, o.eventChannel, err = zk.Connect(o.Endpoints, o.SessionTimeout)
	o.conn.ReconnectInterval = o.ReconnectInterval
	o.connectEventLoops(false)
	if o.connected {
		go o.connectEventLoops(true)
		timer.Stop()
	}
}

func (o *ZooWatcher) Stop(map[string]interface{}) error {

	o.mutex.Lock()
	defer o.mutex.Unlock()

	if o.conn == nil {
		return fmt.Errorf("not connected yet")
	}

	if o.conn != nil {
		o.conn.Close()
		o.connected = false
	}

	o.notifyConnected(false)

	return nil
}

func (o *ZooWatcher) AddConnectCallback(callback ZooConnectCallback) {
	if callback == nil {
		panic("null connect callback")
	}
	o.mutex.Lock()
	defer o.mutex.Unlock()

	if o.connectCallbacks == nil {
		o.connectCallbacks = []ZooConnectCallback{callback}
	} else {
		o.connectCallbacks = append(o.connectCallbacks, callback)
	}
}

func (o *ZooWatcher) connectEventLoops(loop bool) {

	for event := range o.eventChannel {
		var connectevt = false
		switch event.State {
		case zk.StateConnected, zk.StateConnectedReadOnly:
			o.connected = true
			connectevt = true
			o.notifyConnected(true)
		case zk.StateDisconnected:
			o.connected = false
			connectevt = true
		}

		if o.connectCallbacks == nil {
			continue
		}

		for _, callback := range o.connectCallbacks {
			func() {
				defer func() {
					var pan = recover()
					if pan == nil {
						return
					}
					var err = util.AsError(pan)
					callback(&event, o, err)
				}()
				callback(&event, o, nil)
			}()
		}

		if connectevt && !loop {
			return
		}

	}
}

func (o *ZooWatcher) getWatch(wtype WatchType, path string, lock bool) *WatchBox {
	if lock {
		o.watchMutex.RLock()
		defer o.watchMutex.RUnlock()
	}
	switch wtype {
	case WatchTypeGet:
		return o.watchGet[path]
	case WatchTypeExist:
		return o.watchExist[path]
	case WatchTypeChildren:
		return o.watchChildren[path]
	}
	return nil
}

func (o *ZooWatcher) Watch(wtype WatchType, path string, data interface{}, routine WatchRoutine) {
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
	box.path = path
	box.routine = routine
	box.control = make(chan bool, 2)
	box.watcher = o
	box.Data = data

	switch wtype {
	case WatchTypeGet:
		o.watchGet[path] = box
	case WatchTypeExist:
		o.watchExist[path] = box
	case WatchTypeChildren:
		o.watchChildren[path] = box
	}

	go box.loop()
}

func (o *ZooWatcher) Create(path string, data []byte, createflags int32, acl []zk.ACL) (bool, error) {
	exists, _, err := o.conn.Exists(path)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}
	_, err = o.conn.Create(path, data, createflags, acl)
	return false, err
}

func (o *ZooWatcher) GetConn() *zk.Conn {
	return o.conn
}

func (o *ZooWatcher) IsConnected() bool {
	return o.connected
}

func (o *ZooWatcher) notifyConnected(value bool) {
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

func (o *ZooWatcher) WaitForConnected() <-chan bool {
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
