package fswatcher

import (
	"fmt"
	"github.com/camsiabor/qcom/util"
	"github.com/fsnotify/fsnotify"
	"path/filepath"
	"sync"
	"time"
)

type FsWatch struct {
	Name          string
	Path          string
	Data          interface{}
	Async         bool
	AsFile        bool
	ReAddDelay    time.Duration
	CompressDelay time.Duration
	Handler       func(event *fsnotify.Event, path string, watch *FsWatch, watcher *FsWatcher, err error)

	looping bool
	watcher *fsnotify.Watcher
}

type FsWatcher struct {
	watchMutex sync.RWMutex
	watches    map[string]*FsWatch
}

func (o *FsWatcher) Add(watch *FsWatch) error {

	if watch == nil {
		return fmt.Errorf("invalid argument fs watch is null")
	}
	var err error

	if len(watch.Path) == 0 {
		if watch.Path, err = filepath.Abs(watch.Name); err != nil {
			return err
		}
	}

	if o.watches == nil {
		func() {
			o.watchMutex.Lock()
			defer o.watchMutex.Unlock()
			if o.watches == nil {
				o.watches = make(map[string]*FsWatch)
			}
		}()
	}

	var current *FsWatch
	func() {
		o.watchMutex.RLock()
		defer o.watchMutex.RUnlock()
		current = o.watches[watch.Path]
	}()

	if current == nil {
		func() {
			o.watchMutex.Lock()
			defer o.watchMutex.Unlock()
			o.watches[watch.Path] = watch
			current = watch
		}()
	} else {
		current.Handler = watch.Handler
	}

	if current.watcher == nil {
		if current.watcher, err = fsnotify.NewWatcher(); err != nil {
			return err
		}
	}

	if !current.looping {
		current.looping = true
		go o.loop(current)
	}

	if err = current.watcher.Add(current.Path); err != nil {
		return err
	}

	return nil
}

func (o *FsWatcher) Remove(abspath string) error {

	if o.watches == nil {
		return fmt.Errorf("is not added : %v", abspath)
	}

	var current *FsWatch
	func() {
		o.watchMutex.RLock()
		defer o.watchMutex.RUnlock()
		current = o.watches[abspath]
	}()
	if current == nil {
		return fmt.Errorf("is not watch : %v", abspath)
	}

	current.looping = false
	if current.watcher != nil {
		_ = current.watcher.Close()
	}

	func() {
		o.watchMutex.Lock()
		defer o.watchMutex.Unlock()
		delete(o.watches, abspath)
	}()

	return nil
}

func (o *FsWatcher) Get(abspath string) *FsWatch {
	if o.watches == nil {
		return nil
	}
	o.watchMutex.RLock()
	defer o.watchMutex.RUnlock()
	return o.watches[abspath]
}

func (o *FsWatcher) Stop() {
	o.watchMutex.Lock()
	defer o.watchMutex.Unlock()
	for _, watch := range o.watches {
		watch.looping = false
		if watch.watcher != nil {
			_ = watch.watcher.Close()
		}
	}
}

func (o *FsWatcher) loop(watch *FsWatch) {

	var compress = watch.CompressDelay > 0
	var compressMap map[string]*fsnotify.Event
	var compressTimer = time.After(time.Hour)

	for watch.looping {
		select {
		case <-compressTimer:
			if compress && compressMap != nil {
				for _, v := range compressMap {
					if watch.Async {
						go o.handle(watch, v)
					} else {
						o.handle(watch, v)
					}
				}
				compressMap = nil
			} else {
				compressTimer = time.After(time.Hour)
			}
		case event, ok := <-watch.watcher.Events:
			if !ok {
				break
			}
			if compress {
				if compressMap == nil {
					compressMap = map[string]*fsnotify.Event{}
				}
				compressMap[event.Name] = &event
				compressTimer = time.After(watch.CompressDelay)
			} else {
				if watch.Async {
					go o.handle(watch, &event)
				} else {
					o.handle(watch, &event)
				}
			}
		case _, ok := <-watch.watcher.Errors:
			if !ok {
				break
			}
		}
	}

}

func (o *FsWatcher) handle(watch *FsWatch, event *fsnotify.Event) {

	if watch.Handler == nil {
		return
	}

	var path string
	if watch.AsFile {
		path = watch.Path
	} else {
		path, _ = filepath.Abs(watch.Path + "/" + event.Name)
	}

	defer func() {
		var pan = recover()
		if pan == nil {
			return
		}

		if watch.ReAddDelay > 0 {
			go func() {
				time.Sleep(watch.ReAddDelay)
				if watch.watcher != nil {
					_ = watch.watcher.Add(watch.Path)
				}
			}()
		}

		watch.Handler(event, path, watch, o, util.AsError(pan))
	}()

	watch.Handler(event, path, watch, o, nil)

}
