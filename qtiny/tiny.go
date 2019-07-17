package qtiny

import (
	"fmt"
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
	GetGuide() *TinyGuide
	GetOptions() TinyOptions
	GetConfig() map[string]interface{}
	GetTina() *Tina
	Post(request *Message) (response *Message, err error)
	NanoLocalRegister(nano *Nano) error
}

type TinyGuide struct {
	Start func(tiny TinyKind, config map[string]interface{}, future *Future)
	Stop  func(tiny TinyKind, config map[string]interface{}, future *Future)
	Err   func(tiny TinyKind, config map[string]interface{}, err error)
}

type Tiny struct {
	util.LazyData

	id    string
	group string
	tina  *Tina
	guide *TinyGuide

	mutex sync.RWMutex

	nanos []*Nano

	flag    TinyFlag
	options TinyOptions
	config  map[string]interface{}
}

func (o *Tiny) GetId() string {
	return o.id
}

func (o *Tiny) GetTina() *Tina {
	return o.tina
}

func (o *Tiny) GetGuide() *TinyGuide {
	return o.guide
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
	if nano == nil {
		return fmt.Errorf("nano is nil")
	}

	o.mutex.Lock()
	defer o.mutex.Unlock()

	var err = o.tina.microroller.NanoLocalRegister(nano)
	if err != nil {
		return err
	}

	if o.nanos == nil {
		o.nanos = []*Nano{nano}
	} else {
		o.nanos = append(o.nanos, nano)
	}

	return nil
}

func (o *Tiny) Stop() {
	if o.nanos == nil {
		return
	}
	for _, nano := range o.nanos {
		_ = nano.CallbackInvoke(NanoEventStop, o, false)
	}
	o.mutex.Lock()
	o.nanos = nil
	o.mutex.Unlock()
}
