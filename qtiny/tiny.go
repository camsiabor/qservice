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

/* ===== TinyKind ============================================================ */

type TinyKind interface {
	util.LazyDataKind
	GetId() string
	GetGroup() string
	GetGuide() TinyGuideKind
	GetOptions() TinyOptions
	GetConfig() map[string]interface{}
	GetTina() *Tina
	Post(request *Message) (response *Message, err error)
	NanoLocalRegister(nano *Nano) error
}

/* ===== TinyGuide ============================================================ */

type TinyGuideEvent int

const (
	TinyGuideEventStart  TinyGuideEvent = 0x0001
	TinyGuideEventStop   TinyGuideEvent = 0x0002
	TinyGuideEventError  TinyGuideEvent = 0x0010
	TinyGuideEventAspect TinyGuideEvent = 0x1000
)

type TinyGuideCallback func(event TinyGuideEvent, tiny TinyKind, guide TinyGuideKind, config map[string]interface{}, future *Future, err error)
type TinyGuideKind interface {
	util.LazyDataKind
	CallbackAdd(callback TinyGuideCallback)
	Invoke(event TinyGuideEvent, tiny TinyKind, config map[string]interface{}, future *Future)
}

type TinyGuide struct {
	util.LazyData
	mutex     sync.Mutex
	Start     TinyGuideCallback
	Stop      TinyGuideCallback
	Err       TinyGuideCallback
	callbacks []TinyGuideCallback
}

func (o *TinyGuide) Invoke(event TinyGuideEvent, tiny TinyKind, config map[string]interface{}, future *Future) {

	o.mutex.Lock()
	defer o.mutex.Unlock()

	defer func() {
		var pan = recover()
		if pan != nil && o.Err != nil {
			var err = util.AsError(pan)
			o.Err(TinyGuideEventError|event, tiny, o, config, future, err)
		}
	}()

	if TinyGuideEventStart == event {
		o.Start(event, tiny, o, config, future, nil)
	} else if TinyGuideEventStop == event {
		o.Stop(event, tiny, o, config, future, nil)
	}

	if o.callbacks == nil {
		return
	}

	for _, callback := range o.callbacks {
		if callback == nil {
			continue
		}
		func() {
			defer func() {
				var pan = recover()
				if pan != nil {
					callback(TinyGuideEventError|event, tiny, o, config, future, util.AsError(pan))
				}
			}()
			callback(event, tiny, o, config, future, nil)
		}()
	}

}

func (o *TinyGuide) CallbackAdd(callback TinyGuideCallback) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	if o.callbacks == nil {
		o.callbacks = []TinyGuideCallback{callback}
	} else {
		o.callbacks = append(o.callbacks, callback)
	}
}

/* ===== Tiny ============================================================ */

type Tiny struct {
	util.LazyData

	id    string
	group string
	tina  *Tina
	guide TinyGuideKind

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

func (o *Tiny) GetGuide() TinyGuideKind {
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

func (o *Tiny) Start(future *Future) {
	if future == nil {
		future = &Future{}
	}
	o.guide.Invoke(TinyGuideEventStart, o, o.config, future)
}

func (o *Tiny) Stop(future *Future) {

	if future == nil {
		future = &Future{}
	}
	o.guide.Invoke(TinyGuideEventStop, o, o.config, future)

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
