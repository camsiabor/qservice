package qtiny

type FutureEvent int

const (
	FutureRoutine FutureEvent = 0
	FutureSucceed FutureEvent = 1
	FutureFail    FutureEvent = 2
	FutureFinally FutureEvent = 3
)

type FutureCallback func(event FutureEvent, future Future) int

type Future interface {
	IsFail() bool
	IsSucceed() bool

	Fail(code int, cause interface{}) error
	Succeed(code int, result interface{}) error

	TryFail(code int, cause interface{}) error
	TrySucceed(code int, result interface{}) error

	Code() int
	Result() interface{}
	ErrCause() interface{}

	GetData(key string) interface{}
	SetData(key string, val interface{}) Future

	GetContext() interface{}
	SetContext(context interface{}) Future

	OnFail(callback FutureCallback) Future
	OnSucceed(callback FutureCallback) Future
	OnFinally(callback FutureCallback) Future

	Then(callback FutureCallback) Future

	Run() Future
	SetRoutine(routine FutureCallback) Future
}
