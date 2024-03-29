package lmod

import (
	"encoding/json"
	"fmt"
	"github.com/camsiabor/golua/lua"
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/qtiny"
	"time"
	"unsafe"
)

type LuaMessageModule struct {
	tina        *qtiny.Tina
	microroller *qtiny.Microroller
}

func (o *LuaMessageModule) RegisterLuaMessageFunc(L *lua.State, tina *qtiny.Tina) error {

	if tina == nil {
		panic("no context tina")
	}

	o.tina = tina
	o.microroller = o.tina.GetMicroroller()

	var registry = map[string]interface{}{}

	// const
	registry["MessageTypeSend"] = qtiny.MessageTypeSend
	registry["MessageTypeFail"] = qtiny.MessageTypeFail
	registry["MessageTypeReply"] = qtiny.MessageTypeReply
	registry["MessageTypeBroadcast"] = qtiny.MessageTypeBroadcast
	registry["MessageTypeMulticast"] = qtiny.MessageTypeMulticast

	registry["MessageFlagLocalOnly"] = qtiny.MessageFlagLocalOnly
	registry["MessageFlagRemoteOnly"] = qtiny.MessageFlagRemoteOnly

	// method

	registry["Post"] = o.msgPost

	registry["Easy"] = msgEasy

	registry["Type"] = msgType
	registry["TypeString"] = msgTypeString

	registry["Address"] = msgAddress
	registry["Session"] = msgSession
	registry["Gatekey"] = msgGatekey

	registry["Sender"] = msgSender
	registry["Receiver"] = msgReceiver

	registry["Data"] = msgData

	registry["String"] = msgString
	registry["ToJson"] = msgToJson
	registry["FromJson"] = msgFromJson

	registry["IsError"] = msgIsError
	registry["Error"] = msgError
	registry["Reply"] = msgReply
	registry["Replier"] = msgReplier
	registry["ReplyErr"] = msgReplyErr
	registry["ReplyTrace"] = msgReplyTrace
	registry["ReplyCode"] = msgReplyCode
	registry["ReplyData"] = msgReplyData

	registry["TraceDepth"] = msgTraceDepth

	return L.TableRegisters("qmsg", registry)

}

/* ===================== message ==================== */

func msgInstance(L *lua.State) *qtiny.Message {
	if !L.IsNumber(1) {
		panic("is not a reference in registry of message (not a number). lua unit index : " + util.AsStr(L.GetData("luaunit_index"), ""))
	}
	var ptrvalue = L.ToInteger(1)
	var ptr = unsafe.Pointer(uintptr(ptrvalue))
	var message = (*qtiny.Message)(ptr)
	return message
}

func msgNew(L *lua.State, tableIndex int) (*qtiny.Message, error) {

	if !L.IsTable(tableIndex) {
		return nil, fmt.Errorf("parameter %v is not a table. current type = %v", tableIndex, L.Typename(tableIndex))
	}

	var message = &qtiny.Message{}
	message.Type = qtiny.MessageTypeSend

	// message type
	msgType, err := L.TableGetInteger(tableIndex, "Type", int(uint32(qtiny.MessageTypeSend)), false)
	if err != nil {
		return nil, fmt.Errorf("invalid message type : " + err.Error())
	}
	message.Type = qtiny.MessageType(msgType)

	// gatekey
	gatekey, err := L.TableGetString(tableIndex, "Gatekey", "", false)
	if err != nil {
		return nil, fmt.Errorf("invalid Gatekey : " + err.Error())

	}
	message.Gatekey = gatekey

	// address
	address, err := L.TableGetString(tableIndex, "Address", "", true)
	if err != nil {
		return nil, fmt.Errorf("invalid address : " + err.Error())

	}
	message.Address = address

	// message data
	data, err := L.TableGetValue(tableIndex, "Data", nil, false)
	if err != nil {
		return nil, fmt.Errorf("invalid address : " + err.Error())

	}
	message.Data = data

	// receiver
	receiver, err := L.TableGetString(tableIndex, "Receiver", "", false)
	if err != nil {
		return nil, fmt.Errorf("invalid receiver : " + err.Error())

	}
	message.Receiver = receiver

	// share flag
	shareFlag, err := L.TableGetInteger(tableIndex, "ShareFlag", 0, false)
	if err != nil {
		return nil, fmt.Errorf("invalid share flag : " + err.Error())

	}
	message.ShareFlag = qtiny.MessageFlag(util.AsUInt32(shareFlag, 0))

	// local flag
	localFlag, err := L.TableGetInteger(tableIndex, "LocalFlag", 0, false)
	if err != nil {
		return nil, fmt.Errorf("invalid local flag : " + err.Error())

	}
	message.LocalFlag = qtiny.MessageFlag(util.AsUInt32(localFlag, 0))

	// timeout
	timeout, err := L.TableGetInteger(tableIndex, "Timeout", 15000, false)
	if err != nil {
		return nil, fmt.Errorf("invalid timeout : " + err.Error())
	}
	message.Timeout = time.Duration(timeout) * time.Millisecond

	// reply handler
	var hasHandler = false
	handlerRef, err := L.TableGetAndRef(tableIndex, "Handler", false, func(L *lua.State, tableIndex int, key string) error {
		if L.IsNil(-1) {
			return nil
		}
		hasHandler = true
		if !L.IsFunction(-1) {
			return fmt.Errorf("handler is not a function. current type = " + L.Typename(-1))
		}
		return nil
	})
	if hasHandler {
		if err == nil {
			message.Handler = func(message *qtiny.Message) {
				var ptrint = uintptr(unsafe.Pointer(message))
				L.RawGeti(lua.LUA_REGISTRYINDEX, handlerRef)
				L.PushInteger(int64(ptrint))
				_ = L.CallHandle(1, 0, func(L *lua.State, pan interface{}) {
					var err = util.AsError(pan)
					if err != nil {
						_ = message.Error(0, err.Error())
					}
				})
			}
		} else {
			return nil, fmt.Errorf("invalid handler : " + err.Error())
		}
	}

	// trace depth
	traceDepth, err := L.TableGetInteger(tableIndex, "TraceDepth", 1, false)
	if err != nil {
		return nil, fmt.Errorf("invalid trace depth : " + err.Error())

	}
	message.TraceDepth = traceDepth
	return message, nil
}

func (o *LuaMessageModule) msgPost(L *lua.State) int {

	message, err := msgNew(L, 1)
	if err != nil {
		L.PushNil()
		L.PushString(err.Error())
		return 2
	}

	response, err := o.microroller.Post(message.Gatekey, message)
	if response == nil {
		L.PushNil()
	} else {
		var responseInt = uintptr(unsafe.Pointer(response))
		L.PushInteger(int64(responseInt))
		L.KeepGoRef(response)
	}
	if err == nil {
		L.PushNil()
	} else {
		L.PushString(err.Error())
	}

	return 2
}

/* ========================= info ============================ */

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

func msgType(L *lua.State) int {
	var message = msgInstance(L)

	var top = L.GetTop()
	// message trace depth getter
	if top == 1 {
		L.PushNumber(util.AsFloat64(message.Type, 0))
		return 1
	}
	// message trace depth setter
	if L.IsNumber(2) {
		var msgtype = L.ToInteger(2)
		message.Type = qtiny.MessageType(util.AsUInt32(msgtype, 0))
		L.PushNil()
	} else {
		L.PushString("parameter not a number")
	}
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
		var depth = L.ToInteger(2)
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

func msgFromJson(L *lua.State) int {
	if !L.IsString(2) {
		L.PushString("paramter 2 is not a string : " + L.Typename(2))
		return 1
	}
	var jsonstr = L.ToString(2)
	var message = msgInstance(L)
	var err = message.FromJson([]byte(jsonstr))
	if err == nil {
		L.PushNil()
	} else {
		L.PushString(err.Error())
	}
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

	//var message = msgInstance(L)
	//luar.Register(L, "", map[string]interface{}{
	//	"theM": message,
	//})

	// TODO
	// L.GetGlobal("theM")

	return 0
}
