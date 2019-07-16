package qtiny

import (
	"fmt"
	"sync"
)

type FutureImpl struct {
	isFail    bool
	isSucceed bool

	code     int
	errCause interface{}
	result   interface{}

	mutex sync.RWMutex
	data  map[string]interface{}

	context interface{}

	routine   FutureCallback
	onFail    FutureCallback
	onSucceed FutureCallback
	onFinally FutureCallback

	prev *FutureImpl
	next *FutureImpl
}

func (o *FutureImpl) ThenFuture(future Future) Future {
	panic("implement me")
}

func (o *FutureImpl) Prev() Future {
	return o.prev
}

func (o *FutureImpl) Next() Future {
	return o.next
}

func (o *FutureImpl) SetRoutine(routine FutureCallback) Future {
	o.routine = routine
	return o
}

func (o *FutureImpl) Run() Future {

	return o
}

func (o *FutureImpl) IsFail() bool {
	return o.isFail
}

func (o *FutureImpl) IsSucceed() bool {
	return o.isSucceed
}

func (o *FutureImpl) Fail(code int, errCause interface{}) error {
	o.code = code
	o.errCause = errCause
	o.isFail = true
	o.isSucceed = false
	o.forward()
	return nil
}

func (o *FutureImpl) Succeed(code int, result interface{}) error {
	o.code = code
	o.result = result
	o.isFail = false
	o.isSucceed = true
	o.forward()
	return nil
}

func (o *FutureImpl) TryFail(code int, cause interface{}) error {
	if o.isSucceed || o.isFail {
		return fmt.Errorf("already completed. succeed %v. fail %v", o.isSucceed, o.isFail)
	}
	return o.Fail(code, cause)
}

func (o *FutureImpl) TrySucceed(code int, result interface{}) error {
	if o.isSucceed || o.isFail {
		return fmt.Errorf("already completed. succeed %v. fail %v", o.isSucceed, o.isFail)
	}
	return o.Succeed(code, result)
}

func (o *FutureImpl) Code() int {
	return o.code
}

func (o *FutureImpl) Result() interface{} {
	return o.result
}

func (o *FutureImpl) ErrCause() interface{} {
	return o.errCause
}

func (o *FutureImpl) GetData(key string) interface{} {
	if o.data == nil {
		return nil
	}
	var val interface{}
	o.mutex.RLock()
	val = o.data[key]
	o.mutex.RUnlock()
	return val
}

func (o *FutureImpl) SetData(key string, val interface{}) Future {
	if val == nil {
		return o
	}
	if o.data == nil {
		o.mutex.Lock()
		if o.data == nil {
			o.data = make(map[string]interface{})
		}
		o.mutex.Unlock()
	}
	o.mutex.Lock()
	o.data[key] = val
	o.mutex.Unlock()
	return o
}

func (o *FutureImpl) GetContext() interface{} {
	return o.context
}

func (o *FutureImpl) SetContext(context interface{}) Future {
	o.context = context
	return o
}

func (o *FutureImpl) OnFail(callback FutureCallback) Future {
	o.onFail = callback
	return o
}

func (o *FutureImpl) OnSucceed(callback FutureCallback) Future {
	o.onSucceed = callback
	return o
}

func (o *FutureImpl) OnFinally(callback FutureCallback) Future {
	o.onFinally = callback
	return o
}

func (o *FutureImpl) Then(callback FutureCallback) Future {
	return o
}

func (o *FutureImpl) forward() {

	defer func() {
		if o.onFinally != nil {
			o.onFinally(FutureEventFinally, o)
		}
	}()

	if o.isSucceed {
		if o.onSucceed != nil {
			o.onSucceed(FutureEventSucceed, o)
		}
	} else {
		if o.onFail != nil {
			o.onFail(FutureEventSucceed, o)
		}
	}
}
