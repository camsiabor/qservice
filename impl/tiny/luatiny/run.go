package luatiny

import (
	"github.com/camsiabor/golua/lua"
	"github.com/camsiabor/golua/luar"
	"github.com/camsiabor/qcom/qerr"

	"github.com/camsiabor/qcom/util"

	"strings"
)

func InitLuaState(luaPath, luaCPath string) (L *lua.State, err error) {

	defer func() {
		var pan = recover()
		if pan != nil {
			if L != nil {
				L.Close()
			}
			var ok bool
			err, ok = pan.(error)
			if !ok {
				err = qerr.StackStringErr(0, 1024, "lua init state error %v", pan)
			}
		}
	}()

	L = luar.Init()

	L.OpenBase()
	L.OpenLibs()
	L.OpenTable()
	L.OpenString()
	L.OpenPackage()
	L.OpenOS()
	L.OpenMath()
	L.OpenDebug()
	L.OpenBit32()
	L.OpenDebug()

	err = luar.SetLuaPath(L, luaPath, luaCPath)

	return L, err
}

func DefaultErrHandler(L *lua.State, pan interface{}) {
	if pan == nil {
		return
	}
	var ok bool
	L.Notice, ok = pan.(*lua.Interrupt)
	if !ok {
		var stackinfo = qerr.StackCuttingMap(5, 32)
		var stackstr = util.AsStr(stackinfo["stack"], "")
		stackstr = strings.Replace(stackstr, "\t", "  ", -1)
		stackinfo["stack"] = strings.Split(stackstr, "\n")
		L.SetData("err_stack", stackinfo)
	}
}

func RunLuaFile(L *lua.State, filename string, errhandler lua.LuaGoErrHandler) (rets []interface{}, err error) {

	var fpath = filename
	L.GetGlobal(luar.LUA_PATH_ABS)
	if L.IsString(-1) {
		var abs = L.ToString(-1)
		fpath = abs + filename
	}
	L.Pop(-1)

	var topBefore = L.GetTop()
	if err = L.LoadFileEx(fpath); err != nil {
		return rets, err
	}

	if errhandler == nil {
		errhandler = DefaultErrHandler
	}

	err = L.CallHandle(0, lua.LUA_MULTRET, errhandler)
	var topAfter = L.GetTop()
	var returnNum = topAfter - topBefore
	if err == nil {
		if returnNum > 0 {
			rets = make([]interface{}, returnNum)
			for i := 0; i < returnNum; i++ {
				rets[i], err = luar.GetVal(L, i+1)
				if err != nil {
					break
				}
			}
		}
	}
	topAfter = L.GetTop()
	if topAfter-topBefore > 0 {
		for i := 0; i < returnNum; i++ {
			L.Pop(-1)
		}
	}

	if err != nil {
		var qrace = L.GetData("qrace")
		if qrace != nil {
			rets = make([]interface{}, 1)
			rets[0] = qrace
		}
	}

	return rets, err
}
