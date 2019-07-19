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
	L *lua.State

	guide *LuaTinyGuide

	err    error
	name   string
	main   string
	path   string
	config map[string]interface{}
}

func (o *unit) start() {

	defer func() {
		var pan = recover()
		if pan != nil {
			o.err = util.AsError(pan)
		}
		if o.err != nil {
			var logger = o.guide.tiny.GetTina().GetLogger()
			var msg = fmt.Sprintf("lua tiny guide start %v : %v error %v", o.name, o.main, o.err.Error())
			if logger == nil {
				log.Println(msg)
			} else {
				logger.Println(msg)
			}
		}
	}()

	o.path, o.err = filepath.Abs(o.guide.LuaPath + "/" + o.main)
	_, o.err = RunLuaFile(o.L, o.main, func(L *lua.State, pan interface{}) {
		o.err = util.AsError(pan)
	})

}

func (o *unit) init() {

	if len(o.main) == 0 {
		o.main = util.GetStr(o.config, "", "main")
	}

	if len(o.main) == 0 {
		// TODO stop
		return
	}

	if o.L != nil {
		return
	}

	o.L, o.err = InitLuaState(o.guide.LuaPath, o.guide.LuaCPath)
	if o.err != nil {
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
}

func (o *unit) nanoLocalRegister(L *lua.State) int {

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

	nano.CallbackAdd(func(event qtiny.NanoEvent, nano *qtiny.Nano, context interface{}) {

	})

	err = o.guide.tiny.NanoLocalRegister(nano)

	if err == nil {
		L.PushNil()
		log.Println("lua service register", address)
	} else {
		L.PushString(err.Error())
		log.Println("lua service register fail ", address, err.Error())
	}

	return 1

}

func (o *unit) stop() {

}
