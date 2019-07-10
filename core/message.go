package core

type MessageOptions map[string]interface{}
type MessageHeaders map[string]interface{}

type MessageHandler func(message *Message)

type MessageType int

const (
	SEND      MessageType = 0x0001
	REPLY     MessageType = 0x0002
	FAIL      MessageType = 0x0004
	BROADCAST MessageType = 0x1000
)

type Message struct {
	Type MessageType

	Address string
	Data    interface{}
	Timeout int64

	Err    error
	Sender string

	Headers MessageHeaders
	Options MessageOptions

	ReplyId      uint64
	ReplyErr     string
	ReplyCode    int
	ReplyData    interface{}
	ReplyChannel chan interface{}

	Handler MessageHandler

	overseer *Overseer

	Related *Message
}

func NewMessage(address string, data interface{}, timeout int64, headers MessageHeaders, options MessageOptions) (message *Message) {

	message = &Message{}
	message.Address = address
	message.Data = data
	message.Timeout = timeout
	message.Headers = headers
	message.Options = options

	return message
}

func (o *Message) Reply(code int, data interface{}) error {
	o.Type = REPLY
	o.ReplyCode = code
	o.ReplyData = data
	o.Address = o.Sender
	_, err := o.overseer.Post(o)
	return err
}

func (o *Message) IsError() bool {
	return len(o.ReplyErr) > 0 || ((o.Type & FAIL) > 0)
}

func (o *Message) Error(code int, errmsg string) error {
	o.Type = REPLY | FAIL
	o.ReplyCode = code
	o.ReplyErr = errmsg
	_, err := o.overseer.Post(o)
	return err
}

func (o *Message) Overseer() *Overseer {
	return o.overseer
}

func (o *Message) SetHandler(handler MessageHandler) *Message {
	o.Handler = handler
	return o
}
