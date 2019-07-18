package zookeeper

import (
	"fmt"
	"github.com/camsiabor/go-zookeeper/zk"
	"github.com/camsiabor/qcom/util"
	"log"
	"time"
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
	Logger  *log.Logger
	Data    interface{}
}

func (o *WatchBox) GetType() WatchType {
	return o.wtype
}

func (o *WatchBox) GetPath() string {
	return o.path
}

func (o *WatchBox) loop() {

	var ok bool
	var err error
	var data interface{}

	var stat *zk.Stat
	var event zk.Event
	var ch <-chan zk.Event

	event.Path = o.path
	event.Type = zk.EventSession
	event.State = zk.StateHasSession
	event.Err = nil

	for {

		var connectChannel = o.watcher.WaitForConnected()
		if connectChannel != nil {
			var chosen, connected, recvok = util.Timeout(connectChannel, time.Duration(10)*time.Minute)
			if chosen < 0 {
				continue
			}
			if !connected.Bool() || !recvok {
				break
			}
		}

		switch o.wtype {
		case WatchTypeGet:
			data, stat, ch, err = o.watcher.conn.GetW(o.path)
		case WatchTypeExist:
			data, stat, ch, err = o.watcher.conn.ExistsW(o.path)
		case WatchTypeChildren:
			data, stat, ch, err = o.watcher.conn.ChildrenW(o.path)
		}
		if !o.run(&event, stat, data, err) {
			break
		}
		event, ok = <-ch
		if !ok {
			err = fmt.Errorf("closed")
		}
		if event.Type == zk.EventNotWatching {
			if o.Logger != nil {
				o.Logger.Println("zookeeper watcher event not watching ", o.path)
			}
		}
	}

	if o.Logger != nil {
		o.Logger.Println("zookeeper watcher loop end", o.path)
	}

}

func (o *WatchBox) run(event *zk.Event, stat *zk.Stat, data interface{}, err error) (ret bool) {
	defer func() {
		var pan = recover()
		if pan == nil {
			return
		}
		err = util.AsError(pan)
		ret = o.routine(event, stat, data, o, o.watcher, err)
	}()
	return o.routine(event, stat, data, o, o.watcher, err)
}
