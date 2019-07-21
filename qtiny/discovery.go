package qtiny

import "log"

type Discovery interface {
	GetId() string

	Start(config map[string]interface{}) error
	Stop(config map[string]interface{}) error

	GetLogger() *log.Logger
	SetLogger(logger *log.Logger)
}
