package luatiny

import (
	"encoding/json"
	"github.com/camsiabor/qcom/qref"
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/impl/util/fswatcher"
	"github.com/fsnotify/fsnotify"
	"io/ioutil"
	"path/filepath"
	"time"
)

func (o *LuaTinyGuide) watcherStart() {

	defer func() {
		var pan = recover()
		if pan != nil {
			o.Logger.Println(qref.StackString(2))
		}
	}()

	if o.watcherConfig == nil {
		o.watcherConfig = &fswatcher.FsWatcher{}
	}
	if o.watcherScript == nil {
		o.watcherScript = &fswatcher.FsWatcher{}
	}

	/*
		var watch = &fswatcher.FsWatch{
			Path: o.ConfigPathAbs,
			AsFile: true,
			ReAddDelay: time.Second,
			Handler: o.onConfigChange,
		}

		if err := o.watcherConfig.Add(watch); err != nil {
			o.Logger.Println(err)
		}
	*/

	_ = o.watcherConfig.Add(&fswatcher.FsWatch{
		Name:          filepath.Dir(o.ConfigPathAbs),
		ReAddDelay:    time.Second,
		CompressDelay: time.Second,
		Handler:       o.onConfigChange,
	})

	func() {
		o.unitMutex.RLock()
		defer o.unitMutex.RUnlock()
		for _, v := range o.units {
			if v.L == nil {
				continue
			}
			var watch = &fswatcher.FsWatch{
				Path:       v.path,
				AsFile:     true,
				ReAddDelay: time.Second,
				Handler:    o.onScriptChange,
			}
			if err := o.watcherScript.Add(watch); err != nil {
				o.Logger.Println(err)
			}
		}
	}()

}

func (o *LuaTinyGuide) watcherStop() {
	if o.watcherConfig != nil {
		o.watcherConfig.Stop()
	}
	if o.watcherScript != nil {
		o.watcherScript.Stop()
	}
}

func (o *LuaTinyGuide) onConfigChange(event *fsnotify.Event, path string, watch *fswatcher.FsWatch, watcher *fswatcher.FsWatcher, err error) {

	if err != nil {
		o.Logger.Println(err)
		return
	}

	defer func() {
		var pan = recover()
		if pan != nil {
			err = util.AsError(pan)
		}
		if err != nil {
			o.Logger.Println("config file change error", err.Error())
		}
	}()

	o.Logger.Println("[config] ", path, event.String())

	if path != o.ConfigPathAbs {
		return
	}

	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return
	}

	var config map[string]interface{}
	err = json.Unmarshal(bytes, &config)
	if err != nil {
		return
	}

	o.mutex.Lock()
	defer o.mutex.Unlock()
	var meta = util.GetMap(config, true, "meta")
	if err = o.parseMeta(meta); err != nil {
		return
	}

	var changes = make(map[string]interface{})
	for k, update := range config {

		var current = o.Config[k]
		if current == nil {
			changes[k] = current
			continue
		}

		var updateStr = json.Marshal(update)
	}

}

func (o *LuaTinyGuide) onScriptChange(event *fsnotify.Event, path string, watch *fswatcher.FsWatch, watcher *fswatcher.FsWatcher, err error) {
	if err != nil {
		o.Logger.Println(err)
		return
	}
	o.Logger.Println(event.String(), path)
}
