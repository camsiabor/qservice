package luatiny

import (
	"encoding/json"
	"github.com/camsiabor/qcom/qerr"
	"github.com/camsiabor/qcom/qio"
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
			o.Logger.Println(qerr.StackString(2, 1024, ""))
		}
	}()

	if o.watcherConfig == nil {
		o.watcherConfig = &fswatcher.FsWatcher{}
	}
	if o.watcherScript == nil {
		o.watcherScript = &fswatcher.FsWatcher{}
	}

	var err = o.watcherConfig.Add(&fswatcher.FsWatch{
		Name:          filepath.Dir(o.ConfigPathAbs),
		ReAddDelay:    time.Second,
		CompressDelay: o.ReloadDelay,
		Handler:       o.onConfigChange,
	})
	if err != nil {
		o.Logger.Println(err)
	}

	func() {
		o.unitMutex.RLock()
		defer o.unitMutex.RUnlock()
		for _, v := range o.units {
			if v.L == nil {
				continue
			}
			var watch = &fswatcher.FsWatch{
				Path:          v.path,
				AsFile:        true,
				ReAddDelay:    time.Second,
				CompressDelay: o.ReloadDelay,
				Handler:       o.onScriptChange,
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

	if path != o.ConfigPathAbs {
		return
	}

	o.Logger.Println("[lua tiny config change]", event.String())

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

		if k == "meta" {
			continue
		}

		if update == nil {
			changes[k] = nil
			continue
		}
		var current = o.Config[k]
		if current == nil {
			changes[k] = update
			continue
		}
		var updateData, _ = json.Marshal(update)
		var currentData, _ = json.Marshal(current)

		if updateData == nil {
			if currentData != nil {
				changes[k] = current
			}
			continue
		}

		if !qio.BytesEqual(updateData, currentData) {
			changes[k] = update
		}
	}

	for k := range o.Config {
		if config[k] == nil || config[k] == false {
			changes[k] = nil
		}
	}

	for name, change := range changes {
		o.Config[name] = change
	}

	for name, change := range changes {
		var config = util.AsMap(change, false)
		var instance = util.GetInt(config, 1, "instance")
		var unit = o.luaunitGet(name, config, instance)
		var active = util.GetBool(change, true, "active")
		var main = util.GetStr(change, "", "main")
		if !active || len(main) == 0 {
			if unit != nil {
				o.luaunitRemove(name)
			}
			continue
		}
		unit.main = main
		if err = unit.start(true); err != nil {
			o.Logger.Println(unit.string(), err.Error())
		}
	}

}

func (o *LuaTinyGuide) onScriptChange(event *fsnotify.Event, path string, watch *fswatcher.FsWatch, watcher *fswatcher.FsWatcher, err error) {
	if err != nil {
		o.Logger.Println(err)
		return
	}

	if path != watch.Path {
		return
	}

	o.Logger.Println("[lua tiny script change]", event.String())

	o.unitMutex.Lock()
	defer o.unitMutex.Unlock()

	for _, unit := range o.units {
		if unit.path == path {
			if unit.L != nil {
				var err = unit.start(true)
				if err != nil {
					o.Logger.Println(unit.string(), "start fail", err.Error())
				}
			}
		}
	}

}
