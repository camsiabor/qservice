package lmod

import (
	"encoding/json"
	"github.com/camsiabor/golua/lua"
	"github.com/camsiabor/golua/luar"
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/qtiny"
	"unsafe"
)

func RegisterLuaMessageFunc(registry map[string]interface{}) {

	registry["New"] = msgNew

	registry["Easy"] = msgEasy

	registry["Address"] = msgAddress
	registry["Session"] = msgSession
	registry["TypeString"] = msgTypeString
	registry["Gatekey"] = msgGatekey

	registry["Sender"] = msgSender
	registry["Receiver"] = msgReceiver

	registry["Data"] = msgData

	registry["String"] = msgString
	registry["ToJson"] = msgToJson

	registry["IsError"] = msgIsError
	registry["Error"] = msgError
	registry["Reply"] = msgReply
	registry["Replier"] = msgReplier
	registry["ReplyErr"] = msgReplyErr
	registry["ReplyTrace"] = msgReplyTrace
	registry["ReplyCode"] = msgReplyCode
	registry["ReplyData"] = msgReplyData

	registry["TraceDepth"] = msgTraceDepth

}

/* ===================== message ==================== */

func msgInstance(L *lua.State) *qtiny.Message {
	if !L.IsNumber(1) {
		panic("is not reference registry of message")
	}
	var ptrvalue = L.ToInteger(1)
	var ptr = unsafe.Pointer(uintptr(ptrvalue))
	var message = (*qtiny.Message)(ptr)
	return message
}

func msgNew(L *lua.State) {
	/*
		var message = &qtiny.Message{}
		message.Address = address
		message.Data = data
		message.Timeout = timeout

	*/
}

func msgAddress(L *lua.State) int {
	var message = msgInstance(L)
	L.PushString(message.Address)
	return 1
}

func msgSession(L *lua.State) int {
	var message = msgInstance(L)
	L.PushString(message.Session)
	return 1
}

func msgTraceDepth(L *lua.State) int {
	var message = msgInstance(L)

	var top = L.GetTop()
	// message trace depth getter
	if top == 1 {
		L.PushNumber(float64(message.TraceDepth))
		return 1
	}
	// message trace depth setter
	if L.IsNumber(2) {
		var depth = L.ToNumber(2)
		message.TraceDepth = int(depth)
		L.PushNil()
	} else {
		L.PushString("parameter not a number")
	}
	return 1
}

func msgTypeString(L *lua.State) int {
	var message = msgInstance(L)
	L.PushString(message.TypeString())
	return 1
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

func msgToJson(L *lua.State) int {
	var message = msgInstance(L)
	var bytes, err = message.ToJson()
	if err == nil {
		L.PushString(string(bytes))
		L.PushNil()
	} else {
		L.PushNil()
		L.PushString(err.Error())
	}

	return 2
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

	var message = msgInstance(L)

	var top = L.GetTop()
	// message data getter
	if top == 1 {
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

	// message data setter
	var invalidType = false
	if L.IsString(2) {
		message.Data = L.ToString(2)
	} else if L.IsNil(2) {
		message.Data = nil
	} else if L.IsNumber(2) {
		message.Data = L.ToNumber(2)
	} else if L.IsBoolean(2) {
		message.Data = L.ToBoolean(2)
	} else {
		invalidType = true
	}

	if invalidType {
		L.PushString("invalid parameters type")
	} else {
		L.PushNil()
	}

	return 1

}

/* ============ reply ================== */

func msgIsError(L *lua.State) int {
	var message = msgInstance(L)
	L.PushBoolean(message.IsError())
	return 1
}

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

func msgReplyData(L *lua.State) int {
	var message = msgInstance(L)
	if message.ReplyData == nil {
		L.PushNil()
		return 1
	}
	var str, ok = message.ReplyData.(string)
	if ok {
		L.PushString(str)
	} else {
		var bytes, err = json.Marshal(message.ReplyData)
		if err == nil {
			L.PushString(string(bytes))
		} else {
			str = util.AsStr(message.ReplyData, "")
			L.PushString(str)
		}
	}
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

	// TODO
	// L.GetGlobal("theM")

	return 0
}
