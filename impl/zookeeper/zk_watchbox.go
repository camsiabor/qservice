package zookeeper

import (
	"fmt"
	"github.com/camsiabor/qcom/util"
	"github.com/samuel/go-zookeeper/zk"
)

type WatchType int

const (
	WatchTypeExist    WatchType = 1
	WatchTypeGet      WatchType = 2
	WatchTypeChildren WatchType = 3
)

type WatchRoutine func(event *zk.Event, stat *zk.Stat, data interface{}, box *WatchBox, watcher *ZooWatcher, err error) bool

type WatchBox struct {
	wtype   WatchType
	path    string
	control chan bool
	routine WatchRoutine
	watcher *ZooWatcher
}

func (o *WatchBox) GetType() WatchType {
	return o.wtype
}

func (o *WatchBox) GetPath() string {
	return o.path
}

func (o *WatchBox) loop() {
	var err error
	var data interface{}
	var stat *zk.Stat
	var ch <-chan zk.Event
	for {
		o.watcher.WaitForConnected()

		switch o.wtype {
		case WatchTypeGet:
			data, stat, ch, err = o.watcher.conn.GetW(o.path)
		case WatchTypeExist:
			data, stat, ch, err = o.watcher.conn.ExistsW(o.path)
		case WatchTypeChildren:
			data, stat, ch, err = o.watcher.conn.ChildrenW(o.path)
		}
		if err != nil {
			if !o.routine(nil, stat, data, o, o.watcher, err) {
				break
			}
		}
		var event, ok = <-ch
		if !ok {
			err = fmt.Errorf("closed")
		}
		if !o.routine(&event, stat, data, o, o.watcher, err) {
			break
		}
	}
}

func (o *WatchBox) run(event *zk.Event, stat *zk.Stat, data interface{}, err error) {
	defer func() {
		var pan = recover()
		if pan == nil {
			return
		}
		err = util.AsError(pan)
		o.routine(event, stat, data, o, o.watcher, err)
	}()
	o.routine(event, stat, data, o, o.watcher, err)
}
