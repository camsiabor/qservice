package qtiny

import (
	"fmt"
	"github.com/camsiabor/qcom/util"
	"sync"
	"time"
)

type FutureEvent int
type FutureCallbackReturn int

const (
	FutureEventRoutine FutureEvent = 0x0001
	FutureEventThen    FutureEvent = 0x0010
	FutureEventSucceed FutureEvent = 0x1000
	FutureEventFail    FutureEvent = 0x2000
	FutureEventFinally FutureEvent = 0x4000
)

const (
	FutureCallbackContinue  FutureCallbackReturn = 0x0000
	FutureCallbackTerminate FutureCallbackReturn = 0x0001
)

type FutureCallback func(event FutureEvent, future *Future) FutureCallbackReturn

type Future struct {
	util.LazyData

	Name string

	isFail    bool
	isSucceed bool

	code     int
	errCause interface{}
	result   interface{}

	mutex sync.RWMutex

	context interface{}

	routine FutureCallback

	thens []FutureCallback

	onFail    FutureCallback
	onSucceed FutureCallback
	onFinally FutureCallback

	channel chan *Future
}

func (o *Future) SetRoutine(routine FutureCallback) *Future {
	o.routine = routine
	return o
}

func (o *Future) Run() *Future {
	if o.routine == nil {
		panic("no future routine is set")
	}
	defer func() {
		var pan = recover()
		if pan != nil {
			o.TryFail(0, pan)
		}
	}()

	if o.isFail || o.isSucceed {
		o.forward()
	} else {
		o.routine(FutureEventRoutine, o)
	}

	return o
}

func (o *Future) RunAndWait(timeout time.Duration) (*Future, error) {
	if o.channel == nil {
		o.channel = make(chan *Future)
	}

	go o.Run()

	if timeout > 0 {
		var timer = time.After(timeout)
		select {
		case ret, ok := <-o.channel:
			if !ok {
				return nil, fmt.Errorf("future complete channel is closed %v", o.Name)
			}
			return ret, nil
		case <-timer:
			return nil, fmt.Errorf("wait for future timeout %v", o.Name)
		}
	} else {
		return <-o.channel, nil
	}
}

func (o *Future) IsFail() bool {
	return o.isFail
}

func (o *Future) IsSucceed() bool {
	return o.isSucceed
}

func (o *Future) Fail(code int, errCause interface{}) {
	o.code = code
	o.errCause = errCause
	o.isFail = true
	o.isSucceed = false
	o.forward()
}

func (o *Future) Succeed(code int, result interface{}) {
	o.code = code
	o.result = result
	o.isFail = false
	o.isSucceed = true
	o.forward()
}

func (o *Future) TryFail(code int, cause interface{}) {
	if o.isSucceed || o.isFail {
		return
	}
	o.Fail(code, cause)
}

func (o *Future) TrySucceed(code int, result interface{}) {
	if o.isSucceed || o.isFail {
		return
	}
	o.Succeed(code, result)
}

func (o *Future) Code() int {
	return o.code
}

func (o *Future) Result() interface{} {
	return o.result
}

func (o *Future) ErrCause() interface{} {
	return o.errCause
}

func (o *Future) Err() error {
	return util.AsError(o.errCause)
}

func (o *Future) GetContext() interface{} {
	return o.context
}

func (o *Future) SetContext(context interface{}) *Future {
	o.context = context
	return o
}

func (o *Future) OnFail(callback FutureCallback) *Future {
	o.onFail = callback
	return o
}

func (o *Future) OnSucceed(callback FutureCallback) *Future {
	o.onSucceed = callback
	return o
}

func (o *Future) OnFinally(callback FutureCallback) *Future {
	o.onFinally = callback
	return o
}

func (o *Future) Then(callback FutureCallback) *Future {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	if o.thens == nil {
		o.thens = []FutureCallback{callback}
	} else {
		o.thens = append(o.thens, callback)
	}
	return o
}

func (o *Future) forward() {

	defer func() {
		var pan = recover()
		if pan != nil {
			o.errCause = pan
		}
		if o.onFinally != nil {
			o.onFinally(FutureEventFinally, o)
		}
		if o.channel != nil {
			o.channel <- o
			close(o.channel)
			o.channel = nil
		}
	}()

	if o.thens != nil {
		for _, then := range o.thens {
			if then(FutureEventThen, o) == FutureCallbackTerminate {
				break
			}
		}
	}

	if o.isSucceed {
		if o.onSucceed != nil {
			o.onSucceed(FutureEventSucceed, o)
		}
	} else {
		if o.onFail != nil {
			o.onFail(FutureEventFail, o)
		}
	}
}
