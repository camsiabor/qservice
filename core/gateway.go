package core

import "github.com/camsiabor/qservice/qservice"

type Gateway interface {
	Poll(limit int) (chan *qservice.Message, error)
	Post(message *qservice.Message) error
	Broadcast(message *qservice.Message) error

	ServiceRegister(address string, options qservice.ServiceOptions) error
	ServiceUnregister(address string) error
}
