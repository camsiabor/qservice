package zookeeper

import (
	"github.com/camsiabor/go-zookeeper/zk"
	"github.com/camsiabor/qcom/qchan"
	"github.com/camsiabor/qcom/qerr"
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
	wtype WatchType

	Path   string
	Logger *log.Logger
	Data   interface{}

	routine WatchRoutine
	watcher *ZooWatcher

	looping bool

	ch      <-chan zk.Event
	control chan bool

	interval time.Duration
}

func (o *WatchBox) GetType() WatchType {
	return o.wtype
}

func (o *WatchBox) GetPath() string {
	return o.Path
}

func (o *WatchBox) stop() {
	o.looping = false
	if o.control != nil {
		close(o.control)
		o.control = nil
	}
}

func (o *WatchBox) loop() {

	if o.looping {
		return
	}

	var ok bool
	var err error
	var data interface{}

	var stat *zk.Stat
	var event zk.Event

	event.Path = o.Path
	event.Type = zk.EventSession
	event.State = zk.StateHasSession
	event.Err = nil

	o.looping = true

	o.control = make(chan bool)

	for o.looping {

		var connectChannel = o.watcher.WaitForConnected()
		if connectChannel != nil {
			var chosen, connected, recvok = qchan.Timeout(connectChannel, time.Duration(10)*time.Minute)
			if chosen < 0 {
				continue
			}
			if !connected.Bool() || !recvok {
				break
			}
		}

		switch o.wtype {
		case WatchTypeGet:
			data, stat, o.ch, err = o.watcher.conn.GetW(o.Path)
		case WatchTypeExist:
			data, stat, o.ch, err = o.watcher.conn.ExistsW(o.Path)
		case WatchTypeChildren:
			data, stat, o.ch, err = o.watcher.conn.ChildrenW(o.Path)
		}
		if !o.run(&event, stat, data, err) {
			break
		}

		if o.interval <= 0 {
			o.interval = time.Hour * 24
		}

		var timeout = time.After(o.interval)
		select {
		case event, ok = <-o.ch:
			if !ok {
				err = qerr.StackStringErr(0, 1024, "closed")
			}
		case _, ok = <-o.control:
			if !ok {
				break
			}
		case <-timeout:

		}

	}

	if o.Logger != nil {
		o.Logger.Println("zookeeper watcher loop end", o.Path)
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
