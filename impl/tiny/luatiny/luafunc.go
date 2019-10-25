package luatiny

import (
	"github.com/camsiabor/golua/lua"
	"github.com/camsiabor/golua/luar"
	"github.com/camsiabor/qcom/qerr"
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/qtiny"
	"github.com/twinj/uuid"
	"unsafe"
)

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

	err = o.L.TableRegisters("qtiny", map[string]interface{}{

		// life cycle
		"AddTimer":        o.addTimer,
		"AddCloseHandler": lua.AddCloseHandlerDefault,

		// service
		"NanoLocalRegister": o.nanoLocalRegister,

		// message
		"MsgReply": o.msgReply,
		"MsgError": o.msgError,
		"MsgEasy":  o.msgEasy,
	})

	return err
}

/* ===================== life cycle ==================== */

func (o *luaunit) addTimer(L *lua.State) int {

	return 0
}

/* ===================== message ==================== */

func (o *luaunit) msgReply(L *lua.State) int {
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
}

func (o *luaunit) msgError(L *lua.State) int {
	var ptrvalue = L.ToInteger(1)
	var ptr = unsafe.Pointer(uintptr(ptrvalue))
	var message = (*qtiny.Message)(ptr)
	var code = L.ToInteger(2)
	var reply = L.ToString(3)
	var err = message.Error(code, reply)
	if err == nil {
		L.PushNil()
	} else {
		L.PushString(err.Error())
	}
	return 1
}

func (o *luaunit) msgEasy(L *lua.State) int {
	var ptrvalue = L.ToInteger(1)
	var ptr = unsafe.Pointer(uintptr(ptrvalue))
	var message = (*qtiny.Message)(ptr)

	luar.Register(L, "", map[string]interface{}{
		"theM": message,
	})

	return 0
}

/* ===================== service ==================== */

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

	handlerRef, err := L.TableGetAndRef(-1, "Handler", func(L *lua.State, tableIndex int, key string) error {
		if !L.IsFunction(-1) {
			return qerr.StackStringErr(0, 1024, "is not a function")
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
			L.RawGeti(lua.LUA_REGISTRYINDEX, handlerRef)
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
