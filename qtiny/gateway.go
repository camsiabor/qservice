package qtiny

import "log"

type Gateway interface {
	LifeCycler

	GetId() string

	Poll(limit int) (chan *Message, error)
	Post(message *Message) error
	Broadcast(message *Message) error

	ServiceRegister(address string, options ServiceOptions) error
	ServiceUnregister(address string) error

	GetLogger() *log.Logger
	SetLogger(logger *log.Logger)
}