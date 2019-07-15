package qtiny

import "log"

type Gateway interface {
	GetId() string

	Start(config map[string]interface{}) error
	Stop(config map[string]interface{}) error

	Poll(limit int) (chan *Message, error)
	Post(message *Message) error
	Broadcast(message *Message) error

	NanoLocalRegister(address string, flag NanoFlag, options NanoOptions) error
	NanoLocalUnregister(address string) error

	GetLogger() *log.Logger
	SetLogger(logger *log.Logger)
}
