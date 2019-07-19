package luatiny

import (
	"fmt"
	"github.com/camsiabor/golua/lua"
	"github.com/camsiabor/golua/luar"
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/qtiny"
	"log"
	"path/filepath"
	"unsafe"
)

type unit struct {
	L      *lua.State
	err    error
	name   string
	main   string
	path   string
	config map[string]interface{}
}

func (o *LuaTinyGuide) unitGet(name string, main string, config map[string]interface{}) *unit {

	if o.units == nil {
		panic("units is nil")
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
	one.main = main
	one.config = config

	if o.units == nil {
		panic("units is nil")
	}

	func() {
		o.unitMutex.Lock()
		defer o.unitMutex.Unlock()
		o.units[name] = one
	}()

	return one
}

func (o *LuaTinyGuide) unitStart(name string, main string, config map[string]interface{}) {

	var one = o.unitGet(name, main, config)

	defer func() {
		var pan = recover()
		if pan != nil {
			one.err = util.AsError(pan)
		}
		if one.err != nil {
			var logger = o.tiny.GetTina().GetLogger()
			var msg = fmt.Sprintf("lua tiny guide start %v : %v error %v", name, main, one.err.Error())
			if logger == nil {
				log.Println(msg)
			} else {
				logger.Println(msg)
			}
		}
	}()

	o.unitInit(one)

	one.path, one.err = filepath.Abs(o.LuaPath + "/" + one.main)
	_, one.err = RunLuaFile(one.L, one.main, func(L *lua.State, pan interface{}) {
		one.err = util.AsError(pan)
	})

}

func (o *LuaTinyGuide) unitInit(one *unit) {

	if one.L != nil {
		return
	}

	one.L, one.err = InitLuaState(o.LuaPath, o.LuaCPath)
	if one.err != nil {
		return
	}

	luar.Register(one.L, "", map[string]interface{}{
		"tina": o.tiny.GetTina(),
	})

	one.L.Register("Reply", func(L *lua.State) int {
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

	one.L.Register("NanoLocalRegister", o.nanoLocalRegister)
}

func (o *LuaTinyGuide) nanoLocalRegister(L *lua.State) int {

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

	err = o.tiny.NanoLocalRegister(
		&qtiny.Nano{
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
		},
	)

	if err == nil {
		L.PushNil()
		log.Println("lua service register", address)
	} else {
		L.PushString(err.Error())
		log.Println("lua service register fail ", address, err.Error())
	}

	return 1

}
