package core

type Gateway interface {
	LifeCycler

	Poll(limit int) (chan *Message, error)
	Post(message *Message) error
	Broadcast(message *Message) error

	ServiceRegister(address string, options ServiceOptions) error
	ServiceUnregister(address string) error
}
