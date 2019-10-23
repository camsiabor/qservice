package qtiny

import (
	"encoding/json"
	"fmt"
	"github.com/camsiabor/qcom/qerr"
	"github.com/camsiabor/qcom/util"
	"time"
)

type MessageOptions map[string]interface{}
type MessageHeaders map[string]interface{}

type MessageHandler func(message *Message)

type MessageType uint32
type MessageFlag uint32

const (
	MessageTypeSend      MessageType = 0x0001
	MessageTypeReply     MessageType = 0x0002
	MessageTypeFail      MessageType = 0x0004
	MessageTypeBroadcast MessageType = 0x1000
	MessageTypeMulticast MessageType = 0x2000
)

const (
	MessageFlagLocalOnly  MessageFlag = 0x0001
	MessageFlagRemoteOnly MessageFlag = 0x0002
)

type Message struct {
	Type MessageType

	LocalFlag MessageFlag
	ShareFlag MessageFlag

	Address string
	Gatekey string

	Data     interface{}
	Timeout  time.Duration
	Canceled bool

	Err error

	Sender   string
	Receiver string

	Session        string
	SessionRelated interface{}

	Headers MessageHeaders
	Options MessageOptions

	Replier    string
	ReplyId    uint64
	ReplyErr   string
	ReplyTrace string
	TraceDepth int

	ReplyCode    int
	ReplyData    interface{}
	ReplyChannel chan *Message

	Handler MessageHandler

	microroller *Microroller

	Related *Message
}

var MessageTraceDepthDefault = 3

func NewMessage(address string, data interface{}, timeout time.Duration) (message *Message) {

	message = &Message{}
	message.Address = address
	message.Data = data
	message.Timeout = timeout
	return message
}

func (o *Message) Reply(code int, data interface{}) error {
	if o.ReplyId == 0 {
		return fmt.Errorf("no reply id")
	}

	o.Type = MessageTypeReply

	o.ReplyCode = code
	o.ReplyData = data

	o.Timeout = 0

	if o.microroller == nil {
		o.microroller = GetTina().GetMicroroller()
		if o.microroller == nil {
			panic("no microroller is set in message & tina for Message.Reply()")
		}
	}
	o.Replier = o.microroller.GetTina().GetNodeId()
	_, err := o.microroller.Post(o.Gatekey, o)
	return err
}

func (o *Message) IsError() bool {
	return len(o.ReplyErr) > 0 || ((o.Type & MessageTypeFail) > 0)
}

func (o *Message) Error(code int, errmsg string, args ...interface{}) error {

	o.Type = MessageTypeReply
	o.ReplyCode = code

	o.Timeout = 0

	if args == nil {
		o.ReplyErr = errmsg
	} else {
		o.ReplyErr = fmt.Sprintf(errmsg, args...)
	}
	if o.TraceDepth >= 0 {
		if o.TraceDepth == 0 {
			o.TraceDepth = 1024
		}
		o.ReplyTrace = qerr.StackString(1, o.TraceDepth, errmsg)
	}

	if o.microroller == nil {
		o.microroller = GetTina().GetMicroroller()
		if o.microroller == nil {
			panic("no microroller is set in message & tina for Message.Error()")
		}
	}
	o.Replier = o.microroller.GetTina().GetNodeId()
	_, err := o.microroller.Post(o.Gatekey, o)
	return err
}

func (o *Message) Overseer() *Microroller {
	return o.microroller
}

func (o *Message) SetTimeout(timeout time.Duration) *Message {
	o.Timeout = timeout
	return o
}

func (o *Message) SetHandler(handler MessageHandler) *Message {
	o.Handler = handler
	return o
}

func (o *Message) SetHeaders(headers MessageHeaders) *Message {
	o.Headers = headers
	return o
}

func (o *Message) SetOptions(options MessageOptions) *Message {
	o.Options = options
	return o
}

func (o *Message) WaitReply(timeout time.Duration) (*Message, bool) {

	if timeout <= 0 {
		return nil, false
	}

	if o.ReplyChannel == nil {
		o.ReplyChannel = make(chan *Message)
		defer func() {
			close(o.ReplyChannel)
			o.ReplyChannel = nil
		}()
	}

	var timer = time.After(timeout)

	var response *Message
	select {
	case response = <-o.ReplyChannel:
		return response, false
	case <-timer:
		return nil, true
	}
}

func (o *Message) ToJson() ([]byte, error) {
	var m = o.ToMap()
	return json.Marshal(m)
}

func (o *Message) FromJson(data []byte) error {
	var m map[string]interface{}
	var err = json.Unmarshal(data, &m)
	if err != nil {
		return err
	}
	o.FromMap(m)
	return nil
}

func (o *Message) GetTraceDepth() int {
	if o.TraceDepth < 0 {
		return 0
	} else if o.TraceDepth == 0 {
		return MessageTraceDepthDefault
	}
	return o.TraceDepth
}

func (o *Message) ToMap() map[string]interface{} {
	var m = map[string]interface{}{
		"Type":    o.Type,
		"Address": o.Address,
		"Gatekey": o.Gatekey,

		"ShareFlag": o.ShareFlag,

		"Sender":   o.Sender,
		"Receiver": o.Receiver,

		"Session": o.Session,
		"Data":    o.Data,
		"Timeout": o.Timeout,

		"Replier":   o.Replier,
		"ReplyId":   o.ReplyId,
		"ReplyCode": o.ReplyCode,

		"TraceDepth": o.TraceDepth,
	}
	if o.ReplyData != nil {
		m["ReplyData"] = o.ReplyData
	}
	if len(o.ReplyErr) > 0 {
		m["ReplyErr"] = o.ReplyErr
	}

	if len(o.ReplyTrace) > 0 {
		m["ReplyTrace"] = o.ReplyTrace
	}

	if o.Headers != nil {
		m["Headers"] = o.Headers
	}
	if o.Options != nil {
		m["Options"] = o.Options
	}
	return m
}

func (o *Message) FromMap(m map[string]interface{}) {
	o.Type = MessageType(util.AsUInt32(m["Type"], 0))

	o.Address = util.AsStr(m["Address"], "")
	o.Gatekey = util.AsStr(m["Gatekey"], "")

	o.Sender = util.AsStr(m["Sender"], "")
	o.Receiver = util.AsStr(m["Receiver"], "")

	o.Session = util.AsStr(m["Session"], "")

	o.Data = m["Data"]
	o.ShareFlag = MessageFlag(util.AsUInt32(m["ShareFlag"], 0))

	o.Timeout = time.Duration(util.AsInt64(m["Timeout"], 0))

	o.Headers = util.AsMap(m["Headers"], false)
	o.Options = util.AsMap(m["Options"], false)

	o.Replier = util.AsStr(m["Replier"], "")
	o.ReplyId = util.AsUInt64(m["ReplyId"], 0)
	o.ReplyCode = util.AsInt(m["ReplyCode"], 0)
	o.ReplyData = m["ReplyData"]
	o.ReplyErr = util.AsStr(m["ReplyErr"], "")
	o.ReplyTrace = util.AsStr(m["ReplyTrace"], "")
	o.TraceDepth = util.AsInt(m["TraceDepth"], 0)

}

func (o *Message) Clone() *Message {

	var clone = &Message{
		Type:    o.Type,
		Address: o.Address,
		Gatekey: o.Gatekey,
		Sender:  o.Sender,

		Data: o.Data,

		ShareFlag: o.ShareFlag,

		Headers: o.Headers,
		Options: o.Options,

		Session:        o.Session,
		SessionRelated: o.SessionRelated,

		ReplyId: o.ReplyId,
		Replier: o.Replier,

		TraceDepth: o.TraceDepth,
	}

	if clone.Type&MessageTypeReply > 0 {
		clone.ReplyCode = o.ReplyCode
		clone.ReplyErr = o.ReplyErr
		clone.ReplyData = o.ReplyData
		clone.ReplyErr = o.ReplyErr
		clone.ReplyTrace = o.ReplyTrace
	}

	return clone
}

/* =========================== verbose =================================== */

func (o *Message) ToJsonString() (string, error) {
	var bytes, err = o.ToJson()
	if err != nil {
		return "", err
	}
	return string(bytes), err
}

func (o *Message) String() string {
	return fmt.Sprintf("| session [%v] reply_id [%v] type [%v] address [%v] sender [%v.%v] %v {%v} |", o.Session, o.ReplyId, o.TypeString(), o.Address, o.Sender, o.Gatekey, o.ReplyErr, o.Timeout)
}

func (o *Message) TypeString() string {
	switch o.Type {
	case MessageTypeReply:
		return "reply"
	case MessageTypeMulticast:
		return "multicast"
	case MessageTypeBroadcast:
		return "broadcast"
	case MessageTypeFail:
		return "fail"
	default:
		return "common"
	}
}
