package zookeeper

import (
	"github.com/camsiabor/go-zookeeper/zk"
	"github.com/camsiabor/qcom/qerr"
	"github.com/camsiabor/qcom/qroutine"
	"github.com/camsiabor/qcom/util"

	"log"
	"sync"
	"time"
)

type ZooWatcherCallback func(event *zk.Event, watcher *ZooWatcher, err error)

type ZooWatcher struct {
	Id                string
	Endpoints         []string
	SessionTimeout    time.Duration
	ReconnectInterval time.Duration
	callbacks         []ZooWatcherCallback

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

	Logger *log.Logger
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
		return qerr.StackStringErr(0, 1024, "already connected")
	}

	if o.reconnectTimer == nil {
		o.reconnectTimer = &qroutine.Timer{}
		return o.reconnectTimer.Start(0, o.ReconnectInterval, o.reconnect)
	}

	return nil
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
		return qerr.StackStringErr(0, 1024, "not connected yet")
	}

	if o.conn != nil {
		o.conn.Close()
		o.connected = false
	}

	if o.reconnectTimer != nil {
		o.reconnectTimer.Stop()
		o.reconnectTimer = nil
	}

	o.notifyConnected(false)

	return nil
}

func (o *ZooWatcher) AddCallback(callback ZooWatcherCallback) {
	if callback == nil {
		panic("null connect callback")
	}
	o.mutex.Lock()
	defer o.mutex.Unlock()

	if o.callbacks == nil {
		o.callbacks = []ZooWatcherCallback{callback}
	} else {
		o.callbacks = append(o.callbacks, callback)
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

		if o.callbacks == nil {
			continue
		}

		for _, callback := range o.callbacks {
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

func (o *ZooWatcher) Watch(wtype WatchType, path string, data interface{}, interval time.Duration, routine WatchRoutine) {
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

	box.Path = path
	box.Data = data

	box.wtype = wtype
	box.routine = routine
	box.control = make(chan bool, 2)
	box.watcher = o
	box.interval = interval

	switch wtype {
	case WatchTypeGet:
		o.watchGet[path] = box
	case WatchTypeExist:
		o.watchExist[path] = box
	case WatchTypeChildren:
		o.watchChildren[path] = box
	}
	box.Logger = o.Logger
	go box.loop()
}

func (o *ZooWatcher) UnWatch(wtype WatchType, path string) {

	var box = o.getWatch(wtype, path, true)
	if box == nil {
		return
	}

	box.stop()

	o.watchMutex.Lock()
	defer o.watchMutex.Unlock()
	switch wtype {
	case WatchTypeGet:
		delete(o.watchGet, path)
	case WatchTypeExist:
		delete(o.watchExist, path)
	case WatchTypeChildren:
		delete(o.watchChildren, path)
	}
}

func (o *ZooWatcher) Create(path string, data []byte, createflags int32, acl []zk.ACL, update bool) (bool, error) {

	var exists, stat, err = o.conn.Exists(path)
	if err != nil {
		return false, err
	}

	if update {
		_, err = o.conn.Set(path, data, stat.Version)
	}

	if exists {
		return true, err
	}

	_, err = o.conn.Create(path, data, createflags, acl)

	return update, err
}

func (o *ZooWatcher) Delete(path string, removeself, recursive bool) error {

	var children, stat, err = o.conn.Children(path)
	if children != nil && len(children) > 0 {
		if !recursive {
			return qerr.StackStringErr(0, 1024, "it has children : %v", path)
		}
		var n = len(children)
		for i := 0; i < n; i++ {
			var subpath = path + "/" + children[i]
			_ = o.Delete(subpath, true, true)
		}
	}
	if err != nil {
		return err
	}
	if removeself {
		return o.conn.Delete(path, stat.Version)
	}
	return nil
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
