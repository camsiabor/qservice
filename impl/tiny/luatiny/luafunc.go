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

	if !restart && o.Ls != nil {
		return nil
	}

	o.Ls = make([]*lua.State, o.instance)
	for i := 0; i < o.instance; i++ {
		var err = o.initOne(i, restart)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Luaunit) initOne(index int, restart bool) (err error) {

	o.tina = o.guide.tiny.GetTina()

	L, err := InitLuaState(o.guide.LuaPath, o.guide.LuaCPath)
	if err != nil {
		return
	}
	L.SetData("index", index)

	o.Ls[index] = L

	luar.Register(L, "", map[string]interface{}{
		"tina": o.guide.tiny.GetTina(),
	})

	luar.Register(L, "luaunit", map[string]interface{}{
		"name":   o.name,
		"config": o.config,
		"path":   o.path,
	})

	var tinaRegistry = map[string]interface{}{}
	tinaRegistry["AddTimer"] = o.addTimer
	tinaRegistry["AddCloseHandler"] = lua.AddCloseHandlerDefault
	tinaRegistry["NanoLocalRegister"] = o.nanoLocalRegister

	if err = L.TableRegisters("qtiny", tinaRegistry); err != nil {
		return err
	}

	var msgModule = &lmod.LuaMessageModule{}
	if err = msgModule.RegisterLuaMessageFunc(L, o.tina); err != nil {
		return err
	}

	if err = lmod.RegisterLuaOSFunc(L); err != nil {
		return err
	}

	_, err = RunLuaFile(L, o.main, func(L *lua.State, pan interface{}) {
		err = util.AsError(pan)
	})

	if err != nil {
		return err
	}

	return nil
}

/* ===================== life cycle ==================== */

func (o *Luaunit) addTimer(L *lua.State) int {
	return 0
}

/* ===================== service ==================== */

func (o *Luaunit) nanoLocalRegister(L *lua.State) int {

	if L != o.Ls[0] {
		L.PushNil()
		return 1
	}

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

	// TODO multi thread concurrency Lua State program? (need check)
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
