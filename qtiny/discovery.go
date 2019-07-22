package qtiny

import "log"

type Discovery interface {
	GetId() string
	SetId(string)

	GetLogger() *log.Logger
	SetLogger(logger *log.Logger)

	Start(config map[string]interface{}) error
	Stop(config map[string]interface{}) error

	NanoRemoteRegister(nano *Nano) error
	NanoRemoteUnregister(nano *Nano) error
	NanoRemoteGet(address string) (*Nano, error)

	NanoLocalRegister(nano *Nano) error
	NanoLocalUnregister(nano *Nano) error
	NanoLocalGet(address string) (*Nano, error)

	GatewayPublish(gateway Gateway) error
	GatewayUnpublish(gateway Gateway) error

	PortalGet(address string) PortalKind
}
