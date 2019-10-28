package lmod

import (
	"encoding/json"
	"github.com/camsiabor/golua/lua"
	"github.com/camsiabor/golua/luar"
	"github.com/camsiabor/qservice/qtiny"
	"unsafe"
)

func RegisterLuaMessageFunc(registry map[string]interface{}) {

	registry["Easy"] = msgEasy

	registry["Gatekey"] = msgGatekey
	registry["Sender"] = msgSender
	registry["Receiver"] = msgReceiver

	registry["Data"] = msgData

	registry["String"] = msgString

	registry["Error"] = msgError
	registry["Reply"] = msgReply
	registry["Replier"] = msgReplier
	registry["ReplyErr"] = msgReplyErr
	registry["ReplyCode"] = msgReplyCode

}

/* ===================== message ==================== */

func msgInstance(L *lua.State) *qtiny.Message {
	var ptrvalue = L.ToInteger(1)
	var ptr = unsafe.Pointer(uintptr(ptrvalue))
	var message = (*qtiny.Message)(ptr)
	return message
}

func msgGatekey(L *lua.State) int {
	var message = msgInstance(L)
	L.PushString(message.Gatekey)
	return 1
}

func msgString(L *lua.State) int {
	var message = msgInstance(L)
	L.PushString(message.String())
	return 1
}

func msgSender(L *lua.State) int {
	var message = msgInstance(L)
	L.PushString(message.Sender)
	return 1
}

func msgReceiver(L *lua.State) int {
	var message = msgInstance(L)
	L.PushString(message.Receiver)
	return 1
}

func msgData(L *lua.State) int {

	message := msgInstance(L)

	if message.Data == nil {
		L.PushNil()
		L.PushNil()
		return 2
	}

	var str, ok = message.Data.(string)
	if ok {
		L.PushString(str)
		L.PushNil()
	} else {
		var bytes, err = json.Marshal(message.Data)
		if err == nil {
			L.PushString(string(bytes))
			L.PushNil()
		} else {
			L.PushNil()
			L.PushString(err.Error())
		}
	}
	return 2
}

/* ============ reply ================== */

func msgReplier(L *lua.State) int {
	var message = msgInstance(L)
	L.PushString(message.Replier)
	return 1
}

func msgReplyCode(L *lua.State) int {
	var message = msgInstance(L)
	L.PushInteger(int64(message.ReplyCode))
	return 1
}

func msgReplyErr(L *lua.State) int {
	var message = msgInstance(L)
	L.PushString(message.ReplyErr)
	return 1
}

func msgReplyTrace(L *lua.State) int {
	var message = msgInstance(L)
	L.PushString(message.ReplyTrace)
	return 1
}

func msgReply(L *lua.State) int {
	var message = msgInstance(L)
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

func msgError(L *lua.State) int {
	var message = msgInstance(L)
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

/* =========== convenient ============== */

func msgEasy(L *lua.State) int {
	var message = msgInstance(L)

	luar.Register(L, "", map[string]interface{}{
		"theM": message,
	})

	return 0
}
