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

	watchMutex    sync.RWMutex
	watchGet      map[string]*WatchBox
	watchExist    map[string]*WatchBox
	watchChildren map[string]*WatchBox

	connected   bool
	connectCond sync.Cond
}

func (o *ZooWatcher) Start(config map[string]interface{}) error {

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
		var timeout = util.GetInt64("session.timeout", 10)
		o.SessionTimeout = time.Duration(timeout) * time.Second
	}

	if o.ReconnectInterval <= 0 {
		var interval = util.GetInt64("reconnect.interval", 10)
		o.ReconnectInterval = time.Duration(interval) * time.Second
	}

	return o.Conn()
}

func (o *ZooWatcher) Stop(map[string]interface{}) error {
	if !o.IsConnected() {
		return fmt.Errorf("not connected yet")
	}
	if o.timer != nil {
		o.timer.Stop()
	}
	if o.conn != nil {
		o.conn.Close()
		o.connected = false
	}
	return nil
}

func (o *ZooWatcher) Conn() error {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	if o.IsConnected() {
		return fmt.Errorf("already connected")
	}
	var err error
	o.conn, o.eventChannel, err = zk.Connect(o.Endpoints, o.SessionTimeout)
	if err != nil {
		return err
	}
	o.connected = true
	o.connectCond.Broadcast()

	o.Watch(WatchTypeExist, "/", o.onDisconnect)

	return nil
}

func (o *ZooWatcher) onDisconnect(event *zk.Event, stat *zk.Stat, data interface{}, box *WatchBox, watcher *ZooWatcher, err error) bool {
	if err == nil {
		return true
	}

	watcher.connected = false
	o.timer = &qroutine.Timer{}
	_ = o.timer.Start(0, o.ReconnectInterval, func(timer *qroutine.Timer, err error) {
		_ = o.Conn()
		if o.IsConnected() {
			timer.Stop()
		}
	})

	return true
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

func (o *ZooWatcher) Watch(wtype WatchType, path string, routine WatchRoutine) {
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

func (o *ZooWatcher) GetConn() *zk.Conn {
	return o.conn
}

func (o *ZooWatcher) IsConnected() bool {
	return o.connected
}
