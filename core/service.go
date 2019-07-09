package core

import "github.com/camsiabor/qservice/qservice"

type ServiceHandler func(message *qservice.Message)

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
