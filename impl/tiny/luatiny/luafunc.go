package luatiny

import (
	"github.com/camsiabor/golua/lua"
	"github.com/camsiabor/golua/luar"
	"github.com/camsiabor/qcom/qerr"
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/impl/tiny/luatiny/lmod"
	"github.com/camsiabor/qservice/qtiny"
	"github.com/twinj/uuid"
	"unsafe"
)

func (o *Luaunit) init(restart bool) (err error) {

	if !restart && o.L != nil {
		return nil
	}

	o.tina = o.guide.tiny.GetTina()

	o.L, err = InitLuaState(o.guide.LuaPath, o.guide.LuaCPath)
	if err != nil {
		return
	}

	luar.Register(o.L, "", map[string]interface{}{
		"tina": o.guide.tiny.GetTina(),
	})

	var tinaRegistry = map[string]interface{}{}
	tinaRegistry["Post"] = o.msgPost
	tinaRegistry["AddTimer"] = o.addTimer
	tinaRegistry["AddCloseHandler"] = lua.AddCloseHandlerDefault
	tinaRegistry["NanoLocalRegister"] = o.nanoLocalRegister

	if err = o.L.TableRegisters("qtiny", tinaRegistry); err != nil {
		return err
	}

	var msgRegistry = map[string]interface{}{}
	lmod.RegisterLuaMessageFunc(msgRegistry)
	if err = o.L.TableRegisters("qmsg", msgRegistry); err != nil {
		return err
	}

	return err
}

/* ===================== life cycle ==================== */

func (o *Luaunit) addTimer(L *lua.State) int {
	return 0
}

/* ===================== service ==================== */
func (o *Luaunit) msgPost(L *lua.State) int {

	var microroller = o.tina.GetMicroroller()
	if microroller == nil {
		panic("microroller is null in tina : " + o.tina.GetNodeId())
	}

	var gatekey = L.ToString(1)
	var ptrvalue = L.ToInteger(2)
	if ptrvalue == 0 {
		panic("paramter 2 pointer value is zero")
	}
	var ptr = unsafe.Pointer(uintptr(ptrvalue))
	var message = (*qtiny.Message)(ptr)

	var response, err = microroller.Post(gatekey, message)
	if response == nil {
		L.PushNil()
	} else {
		var responseInt = uintptr(unsafe.Pointer(response))
		L.PushInteger(int64(responseInt))
	}
	if err == nil {
		L.PushNil()
	} else {
		L.PushString(err.Error())
	}
	L.PushNil()
	return 2
}

func (o *Luaunit) nanoLocalRegister(L *lua.State) int {

	if !L.IsTable(1) {
		L.PushString("invalid argument! need a table")
		return 1
	}

	address, err := L.TableGetString(-1, "Address", "", true)
	if err != nil {
		L.PushString("invalid address : " + err.Error())
		return 1
	}
	flag, _ := L.TableGetInteger(-1, "Flag", 0, false)

	handlerRef, err := L.TableGetAndRef(-1, "Handler", true, func(L *lua.State, tableIndex int, key string) error {
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
