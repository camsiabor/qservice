package qtiny

import (
	"log"
)

type GatewayEvent int

const (
	GatewayEventConnected    GatewayEvent = 1
	GatewayEventDisconnected GatewayEvent = 2
	GatewayEventConfigChange GatewayEvent = 3
)

type GatewayEventBox struct {
	Event  GatewayEvent
	Source Gateway
	Meta   interface{}
}

type Gateway interface {
	GetId() string
	SetId(string)

	GetNodeId() string
	SetNodeId(string)

	GetType() string
	GetMeta() map[string]interface{}

	GetConfig() map[string]interface{}
	SetConfig(map[string]interface{})

	GetLogger() *log.Logger
	SetLogger(logger *log.Logger)

	Start(config map[string]interface{}) error
	Stop(config map[string]interface{}) error

	Poll(limit int) (chan *Message, error)

	Post(message *Message, discovery Discovery) error
	Multicast(message *Message, discovery Discovery) error
	Broadcast(message *Message, discovery Discovery) error

	EventChannelGet(channelId string) (<-chan *GatewayEventBox, error)
	EventChannelClose(channelId string) error
}
