package qtiny

import "log"

type Gateway interface {
	GetId() string

	Start(config map[string]interface{}) error
	Stop(config map[string]interface{}) error

	Poll(limit int) (chan *Message, error)
	Post(message *Message) error
	Broadcast(message *Message) error

	NanoLocalRegister(nano *Nano) error
	NanoLocalUnregister(nano *Nano) error

	GetLogger() *log.Logger
	SetLogger(logger *log.Logger)
}
