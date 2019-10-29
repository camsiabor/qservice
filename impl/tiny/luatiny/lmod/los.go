package lmod

import (
	"github.com/camsiabor/golua/lua"
	"time"
)

func RegisterLuaOSFunc(L *lua.State) error {
	var registry = map[string]interface{}{}
	registry["Sleep"] = osSleep
	return L.TableRegisters("qos", registry)

}

func osSleep(L *lua.State) int {
	var duration = L.ToInteger(1)
	time.Sleep(time.Duration(duration) * time.Millisecond)
	return 0
}
