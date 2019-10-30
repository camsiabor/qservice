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

type Luaunit struct {
	Ls       []*lua.State
	instance int

	guide *LuaTinyGuide

	tina *qtiny.Tina

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

func (o *Luaunit) start(restart bool) (err error) {

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

	if o.Ls != nil {
		o.stop(false)
	}

	if !util.GetBool(o.config, true, "active") {
		return nil
	}
	o.instance = util.GetInt(o.config, 1, "instance")
	if o.instance <= 0 {
		return nil
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

	o.err = o.init(restart)

	if o.err == nil {
		o.logger.Println(o.string(), "start")
	}

	return o.err
}

func (o *Luaunit) stop(lock bool) {

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

	if o.Ls != nil {
		for i := 0; i < len(o.Ls); i++ {
			if o.Ls[i] != nil {
				o.Ls[i].Close()
				o.Ls[i] = nil
			}
		}
		o.Ls = nil
		o.instance = 0
	}

	o.logger.Println(o.string(), "stop")
}

func (o *Luaunit) string() string {
	return fmt.Sprintf("[ %v | %v | [%v] | %v ]", o.guide.Name, o.name, o.instance, o.path)
}

func (o *Luaunit) GetTina() *qtiny.Tina {
	return o.tina
}
