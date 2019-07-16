package qtiny

import (
	"github.com/camsiabor/qcom/util"
	"sync"
)

type TinyOptions map[string]interface{}
type TinyFlag int

const (
	TinyFlagDeploySync TinyFlag = 0x1000
)

type TinyKind interface {
	util.LazyDataKind

	GetId() string
	GetGroup() string
	GetOptions() TinyOptions
	GetConfig() map[string]interface{}
	GetTina() *Tina
	Post(request *Message) (response *Message, err error)
	NanoLocalRegister(nano *Nano) error
}

type TinyGuide struct {
	Start func(tiny TinyKind, future Future)
	Stop  func(tiny TinyKind, future Future)
	Err   func(tiny TinyKind, err error)
}

type Tiny struct {
	util.LazyData

	id    string
	group string
	tina  *Tina
	guide *TinyGuide

	mutex sync.RWMutex

	flag    TinyFlag
	options TinyOptions
	config  map[string]interface{}
}

func (o *Tiny) GetTina() *Tina {
	return o.tina
}

func (o *Tiny) GetId() string {
	return o.id
}

func (o *Tiny) GetGroup() string {
	return o.group
}

func (o *Tiny) GetOptions() TinyOptions {
	return o.options
}

func (o *Tiny) GetConfig() map[string]interface{} {
	return o.config
}

func (o *Tiny) Post(request *Message) (*Message, error) {
	return o.tina.microroller.Post(request)
}

func (o *Tiny) NanoLocalRegister(nano *Nano) error {
	return o.tina.microroller.NanoLocalRegister(nano)
}
