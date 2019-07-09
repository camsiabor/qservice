package core

type ServiceHandler func(message *Message)

type ServiceOptions map[string]interface{}

type Service struct {
	address string
	options ServiceOptions
	handler ServiceHandler
	next    *Service
}

func (o *Service) tail() (result *Service) {
	var current = o
	for current.next != nil {
		current = current.next
	}
	return current
}
