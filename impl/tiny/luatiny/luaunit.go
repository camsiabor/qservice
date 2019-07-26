package luatiny

import (
	"fmt"
	"github.com/camsiabor/golua/lua"
	"github.com/camsiabor/qcom/qerr"
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/qtiny"
	"log"
	"path/filepath"
	"sync"
)

type luaunit struct {
	L *lua.State

	guide *LuaTinyGuide

	err  error
	name string
	main string
	path string

	logger *log.Logger

	config map[string]interface{}

	mutex sync.RWMutex

	nanoMutex sync.RWMutex
	nanos     map[string]*qtiny.Nano
}

func (o *luaunit) start(restart bool) (err error) {

	defer func() {
		var pan = recover()
		if pan != nil {
			err = util.AsError(pan)
		}
		if err != nil {
			o.err = err
		}
	}()

	o.mutex.Lock()
	defer o.mutex.Unlock()

	if o.L != nil {
		o.stop(false)
	}

	if !util.GetBool(o.config, true, "active") {
		return nil
	}

	err = o.init(restart)
	if err != nil {
		return err
	}

	if len(o.main) == 0 {
		o.main = util.GetStr(o.config, "", "main")
	}

	if len(o.main) == 0 {
		return qerr.StackStringErr(0, 1024, o.string(), "main is not set")
	}

	o.path, err = filepath.Abs(o.guide.LuaPath + "/" + o.main)
	if err != nil {
		return err
	}

	_, err = RunLuaFile(o.L, o.main, func(L *lua.State, pan interface{}) {
		err = util.AsError(pan)
	})

	o.err = err

	o.logger.Println(o.string(), "start")

	return err
}

func (o *luaunit) stop(lock bool) {

	if lock {
		o.mutex.Lock()
		defer o.mutex.Unlock()
	}

	if o.nanos != nil {
		func() {
			o.nanoMutex.Lock()
			defer o.nanoMutex.Unlock()
			for _, nano := range o.nanos {
				var err = o.guide.tiny.NanoLocalUnregister(nano)
				if err == nil {
					o.logger.Println(o.string(), "lua nano unregister -> ", nano.Address)
				} else {
					o.logger.Println(o.string(), "lua nano unregister fail -> ", nano.Address, " : ", err.Error())
				}
			}
			o.nanos = nil
		}()
	}

	if o.L != nil {
		o.L.Close()
		o.L = nil
	}

	o.logger.Println(o.string(), "stop")
}

func (o *luaunit) string() string {
	return fmt.Sprintf("[ %v @ %v # %v ]", o.guide.Name, o.name, o.path)
}
