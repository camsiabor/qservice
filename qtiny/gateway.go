package qtiny

import "log"

type Gateway interface {
	LifeCycler

	GetId() string

	Poll(limit int) (chan *Message, error)
	Post(message *Message) error
	Broadcast(message *Message) error

	NanoLocalRegister(address string, flag NanoFlag, options NanoOptions) error
	NanoLocalUnregister(address string) error

	GetLogger() *log.Logger
	SetLogger(logger *log.Logger)
}
