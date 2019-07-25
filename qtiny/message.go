package qtiny

import (
	"encoding/json"
	"fmt"
	"github.com/camsiabor/qcom/util"
	"time"
)

type MessageOptions map[string]interface{}
type MessageHeaders map[string]interface{}

type MessageHandler func(message *Message)

type MessageType int
type MessageFlag int

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
	Type      MessageType
	LocalFlag MessageFlag

	Address string
	Gatekey string

	Data     interface{}
	Timeout  time.Duration
	Canceled bool

	Err error

	Sender         string
	Replier        string
	Session        string
	SessionRelated interface{}

	Headers MessageHeaders
	Options MessageOptions

	ReplyId      uint64
	ReplyErr     string
	ReplyCode    int
	ReplyData    interface{}
	ReplyChannel chan *Message

	Handler MessageHandler

	microroller *Microroller

	Related *Message
}

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
	_, err := o.microroller.Post(o.Gatekey, o)
	return err
}

func (o *Message) IsError() bool {
	return len(o.ReplyErr) > 0 || ((o.Type & MessageTypeFail) > 0)
}

func (o *Message) Error(code int, errmsg string) error {
	o.Type = MessageTypeReply | MessageTypeFail
	o.ReplyCode = code
	o.ReplyErr = errmsg
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

func (o *Message) ToMap() map[string]interface{} {
	var m = map[string]interface{}{
		"Type":      o.Type,
		"Address":   o.Address,
		"Gatekey":   o.Address,
		"Sender":    o.Sender,
		"Replier":   o.Replier,
		"Session":   o.Session,
		"Data":      o.Data,
		"Timeout":   o.Timeout,
		"ReplyId":   o.ReplyId,
		"ReplyCode": o.ReplyCode,
	}
	if o.ReplyData != nil {
		m["ReplyData"] = o.ReplyData
	}
	if len(o.ReplyErr) > 0 {
		m["ReplyErr"] = o.ReplyErr
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
	o.Type = MessageType(util.AsInt(m["Type"], 0))
	o.Address = util.AsStr(m["Address"], "")
	o.Gatekey = util.AsStr(m["Gatekey"], "")
	o.Sender = util.AsStr(m["Sender"], "")
	o.Replier = util.AsStr(m["Replier"], "")
	o.Session = util.AsStr(m["Session"], "")
	o.Data = m["Data"]
	o.Timeout = time.Duration(util.AsInt64(m["Timeout"], 0))
	o.ReplyId = util.AsUInt64(m["ReplyId"], 0)
	o.ReplyCode = util.AsInt(m["ReplyCode"], 0)
	o.ReplyData = m["ReplyData"]
	o.ReplyErr = util.AsStr(m["ReplyErr"], "")
	o.Headers = util.AsMap(m["Headers"], false)
	o.Options = util.AsMap(m["Options"], false)
}

func (o *Message) Clone() *Message {
	var clone = &Message{

		Type:    o.Type,
		Address: o.Address,
		Gatekey: o.Gatekey,
		Sender:  o.Sender,

		Data: o.Data,

		Headers: o.Headers,
		Options: o.Options,

		Session:        o.Session,
		SessionRelated: o.SessionRelated,

		ReplyId: o.ReplyId,
		Replier: o.Replier,
	}

	if clone.Type&MessageTypeReply > 0 {
		clone.ReplyCode = o.ReplyCode
		clone.ReplyErr = o.ReplyErr
		clone.ReplyData = o.ReplyData
		clone.ReplyErr = o.ReplyErr
	}

	return clone
}
