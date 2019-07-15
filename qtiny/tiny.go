package qtiny

import (
	"bitbucket.org/avd/go-ipc/sync"
	"github.com/camsiabor/qcom/util"
)

type TinyOptions map[string]interface{}
type TinyFlag int

type TinyKind interface {
	util.LazyDataKind

	GetId() string
	GetGroup() string
	GetOptions() TinyOptions
	GetConfig() map[string]interface{}
	Post(request *Message) (response *Message, err error)
	Register(address string, flag NanoFlag, options NanoOptions, handler NanoHandler) error
}

type TinyGuide struct {
	Start func(tiny TinyKind, future Future) error
	Stop  func(tiny TinyKind, future Future) error
	Err   func(tiny TinyKind, err error)
}

type Tiny struct {
	util.LazyData

	id    string
	group string
	tina  *Tina
	guide *TinyGuide

	mutex sync.RWMutex

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

func (o *Tiny) Register(address string, flag NanoFlag, options NanoOptions, handler NanoHandler) error {
	return o.tina.microroller.NanoLocalRegister(address, flag, options, handler)
}
