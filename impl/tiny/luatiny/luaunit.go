package luatiny

import (
	"fmt"
	"github.com/camsiabor/golua/lua"
	"github.com/camsiabor/golua/luar"
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/qtiny"
	"github.com/twinj/uuid"
	"log"
	"path/filepath"
	"sync"
	"unsafe"
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
		return fmt.Errorf(o.string(), "main is not set")
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

func (o *luaunit) init(restart bool) (err error) {

	if !restart && o.L != nil {
		return nil
	}

	o.L, err = InitLuaState(o.guide.LuaPath, o.guide.LuaCPath)
	if err != nil {
		return
	}

	luar.Register(o.L, "", map[string]interface{}{
		"tina": o.guide.tiny.GetTina(),
	})

	o.L.Register("Reply", func(L *lua.State) int {
		var ptrvalue = L.ToInteger(1)
		var ptr = unsafe.Pointer(uintptr(ptrvalue))
		var message = (*qtiny.Message)(ptr)
		var code = L.ToInteger(2)
		var reply = L.ToString(3)
		var err = message.Reply(code, reply)
		if err == nil {
			L.PushNil()
		} else {
			L.PushString(err.Error())
		}
		return 1
	})

	o.L.Register("NanoLocalRegister", o.nanoLocalRegister)

	return nil
}

func (o *luaunit) nanoLocalRegister(L *lua.State) int {

	if !L.IsTable(-1) {
		L.PushString("invalid argument! need a table")
		return 1
	}

	address, err := L.TableGetString(-1, "Address", "")
	if err != nil {
		L.PushString("invalid address : " + err.Error())
		return 1
	}
	flag, _ := L.TableGetInteger(-1, "Flag", 0)

	handlerReference, err := L.TableGetAndRef(-1, "Handler", func(L *lua.State, tableIndex int, key string) error {
		if !L.IsFunction(-1) {
			return fmt.Errorf("is not a function")
		}
		return nil
	})

	if err != nil {
		L.PushString("invalid handler : " + err.Error())
		return 1
	}

	var nano = &qtiny.Nano{
		Id:      uuid.NewV4().String(),
		Address: address,
		Flag:    qtiny.NanoFlag(flag),
		Options: nil, // TODO options
		Handler: func(message *qtiny.Message) {
			var ptrint = uintptr(unsafe.Pointer(message))
			L.RawGeti(lua.LUA_REGISTRYINDEX, handlerReference)
			L.PushInteger(int64(ptrint))
			_ = L.CallHandle(1, 0, func(L *lua.State, pan interface{}) {
				var err = util.AsError(pan)
				if err != nil {
					_ = message.Error(0, err.Error())
				}
			})
		},
	}

	if o.nanos == nil {
		func() {
			o.nanoMutex.Lock()
			defer o.nanoMutex.Unlock()
			if o.nanos == nil {
				o.nanos = make(map[string]*qtiny.Nano)
			}
		}()
	}

	func() {
		o.nanoMutex.Lock()
		defer o.nanoMutex.Unlock()
		o.nanos[nano.Id] = nano
	}()

	err = o.guide.tiny.NanoLocalRegister(nano)

	if err == nil {
		L.PushNil()
		o.logger.Println(o.string(), "lua nano register -> ", address)
	} else {
		L.PushString(err.Error())
		o.logger.Println(o.string(), "lua nano register fail -> ", address, err.Error())
	}

	return 1

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
