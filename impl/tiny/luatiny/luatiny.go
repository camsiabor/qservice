package luatiny

import (
	"encoding/json"
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/impl/util/fswatcher"
	"github.com/camsiabor/qservice/qtiny"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
)

type LuaTinyGuide struct {
	qtiny.TinyGuide

	Name string

	ConfigPath    string
	ConfigPathAbs string
	Config        map[string]interface{}

	LuaPath  string
	LuaCPath string

	Logger *log.Logger

	mutex sync.RWMutex

	unitMutex sync.RWMutex
	units     map[string]*unit

	tiny qtiny.TinyKind

	watcherConfig *fswatcher.FsWatcher
	watcherScript *fswatcher.FsWatcher
}

func NewLuaTinyGuide(name string, configPath string) *LuaTinyGuide {
	var guide = &LuaTinyGuide{}
	if len(configPath) == 0 {
		panic("lua tiny guide config path is not set")
	}
	guide.Name = name
	guide.ConfigPath = configPath
	guide.ConfigPathAbs, _ = filepath.Abs(configPath)
	guide.TinyGuide.Start = guide.start
	guide.TinyGuide.Stop = guide.stop
	guide.units = make(map[string]*unit)
	return guide
}

func (o *LuaTinyGuide) parseMeta(meta map[string]interface{}) (err error) {
	if len(o.LuaPath) == 0 {
		o.LuaPath = util.GetStr(meta, "../../src/github.com/camsiabor/test/lua/", "path")
		o.LuaPath, err = filepath.Abs(o.LuaPath)
		if err != nil {
			return err
		}
	}

	if len(o.LuaCPath) == 0 {
		o.LuaCPath = util.GetStr(meta, o.LuaPath, "cpath")
		o.LuaCPath, err = filepath.Abs(o.LuaCPath)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *LuaTinyGuide) start(event qtiny.TinyGuideEvent, tiny qtiny.TinyKind, guide qtiny.TinyGuideKind, config map[string]interface{}, future *qtiny.Future, err error) {

	if o.Logger == nil {
		o.Logger = tiny.GetTina().GetLogger()
		if o.Logger == nil {
			o.Logger = log.New(os.Stdout, "[lua.tiny] ", log.LstdFlags|log.Lshortfile)
		}
	}

	if err != nil {
		o.Logger.Printf("zookeeper tiny guide start error %v", err)
		return
	}

	defer func() {
		if err != nil {
			future.TryFail(0, err)
		}
	}()

	o.mutex.Lock()
	defer o.mutex.Unlock()

	o.tiny = tiny

	var bytes []byte
	if bytes, err = ioutil.ReadFile(o.ConfigPath); err != nil {
		return
	}

	if err = json.Unmarshal(bytes, &o.Config); err != nil {
		return
	}

	var meta = util.GetMap(config, true, "meta")
	if err = o.parseMeta(meta); err != nil {
		return
	}

	for k, v := range o.Config {
		if k == "meta" || v == nil {
			continue
		}
		var config = util.AsMap(v, true)

		o.unitStart(k, config)
	}

	o.watcherStart()

}

func (o *LuaTinyGuide) stop(event qtiny.TinyGuideEvent, tiny qtiny.TinyKind, guide qtiny.TinyGuideKind, config map[string]interface{}, future *qtiny.Future, err error) {

	if err != nil {
		o.Logger.Printf("zookeeper tiny guide stop error %v", err)
		return
	}

	o.mutex.Lock()
	defer o.mutex.Unlock()

	o.watcherStop()

	o.unitMutex.Lock()
	defer o.unitMutex.Unlock()

	for _, v := range o.units {
		if v.L != nil {
			v.L.Close()
		}
	}

}

func (o *LuaTinyGuide) unitGet(name string) *unit {

	if o.units == nil {
		func() {
			o.unitMutex.Lock()
			defer o.unitMutex.Unlock()
			if o.units == nil {
				o.units = make(map[string]*unit)
			}
		}()
	}

	var one *unit
	func() {
		o.unitMutex.RLock()
		defer o.unitMutex.RUnlock()
		one = o.units[name]
	}()

	if one != nil {
		return one
	}

	one = &unit{}
	one.name = name
	one.guide = o

	func() {
		o.unitMutex.Lock()
		defer o.unitMutex.Unlock()
		o.units[name] = one
	}()

	return one
}
