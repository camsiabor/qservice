package memory

import (
	"github.com/camsiabor/qcom/qerr"
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/qtiny"
	"github.com/twinj/uuid"
	"hash/fnv"
	"log"
	"sync"
)

type PublishHandler func(messageType qtiny.MessageType,
	portalAddress string, portal qtiny.PortalKind,
	remote *qtiny.Nano, message *qtiny.Message,
	discovery qtiny.Discovery, gateway qtiny.Gateway, data []byte) error

type MemGateway struct {
	Id     string
	IdHash uint32

	NodeId     string
	NodeIdHash uint32

	Type     string
	TypeHash uint32

	Tag string

	Mutex sync.Mutex

	Looping bool
	Logger  *log.Logger

	QueueLimit int
	Queue      chan *qtiny.Message

	Listeners []chan *qtiny.Message

	Meta map[string]interface{}

	Config map[string]interface{}

	EventChannelsMutex sync.RWMutex
	EventChannels      map[string]chan *qtiny.GatewayEventBox

	Publisher PublishHandler
}

func (o *MemGateway) Start(config map[string]interface{}) error {

	if config == nil {
		config = o.Config
	}

	var configId = util.GetStr(config, "", "id")
	if len(configId) > 0 {
		o.Id = configId
	}

	if len(o.Id) == 0 {
		o.Id = uuid.NewV4().String()
	}

	if o.Looping {
		return qerr.StackStringErr(0, 1024, "already running")
	}

	if o.EventChannels == nil {
		o.EventChannels = make(map[string]chan *qtiny.GatewayEventBox)
	}

	if o.Queue == nil {
		o.QueueLimit = util.GetInt(config, 8192, "queue.limit")
		if o.QueueLimit <= 16 {
			o.QueueLimit = 8192
		}
		o.Queue = make(chan *qtiny.Message, o.QueueLimit)
	}

	o.Looping = true
	go o.DispatchLoop()
	return nil
}

func (o *MemGateway) Stop(map[string]interface{}) error {
	o.Looping = false

	if o.Queue != nil {
		close(o.Queue)
		o.Queue = nil
	}

	o.EventChannelSend(qtiny.GatewayEventDisconnected, o.Meta)

	return nil
}

func (o *MemGateway) DispatchLoop() {
	var ok bool
	var msg *qtiny.Message
	for o.Looping {
		select {
		case msg, ok = <-o.Queue:
			if !ok {
				break
			}
			if o.Listeners == nil {
				continue
			}
		}
		if msg != nil {
			var n = len(o.Listeners)
			for i := 0; i < n; i++ {
				o.Listeners[i] <- msg
			}
		}
	}
	o.Looping = false
}

func (o *MemGateway) Poll(limit int) (chan *qtiny.Message, error) {

	if limit <= 0 {
		limit = 8192
	}

	var ch = make(chan *qtiny.Message, limit)

	o.Mutex.Lock()
	defer o.Mutex.Unlock()

	if o.Listeners == nil {
		o.Listeners = make([]chan *qtiny.Message, 1)
		o.Listeners[0] = ch
	} else {
		o.Listeners = append(o.Listeners, ch)
	}

	return ch, nil
}

func (o *MemGateway) Post(message *qtiny.Message, discovery qtiny.Discovery) error {
	if o.Queue == nil {
		return qerr.StackStringErr(0, 1024, "gateway not started yet")
	}
	if message.Type&qtiny.MessageTypeReply > 0 {
		message.Address = message.Sender
	}
	message.Sender = o.Id
	var clone = message.Clone()
	if message.LocalFlag&qtiny.MessageFlagLocalOnly > 0 {
		clone.LocalFlag = qtiny.MessageFlagLocalOnly
	}
	o.Queue <- clone
	return nil
}

func (o *MemGateway) Multicast(message *qtiny.Message, discovery qtiny.Discovery) error {
	message.Type = message.Type | qtiny.MessageTypeMulticast
	return o.Post(message, discovery)
}

func (o *MemGateway) Broadcast(message *qtiny.Message, discovery qtiny.Discovery) error {
	message.Type = message.Type | qtiny.MessageTypeBroadcast
	return o.Post(message, discovery)
}

func (o *MemGateway) IsPortalValid(portal qtiny.PortalKind) bool {
	if portal == nil {
		return false
	}
	if len(portal.GetType()) == 0 {
		return false
	}
	return true
}

func (o *MemGateway) Publish(message *qtiny.Message, discovery qtiny.Discovery) error {

	if o.Queue == nil {
		return qerr.StackStringErr(0, message.GetTraceDepth(), "gateway %v not started yet", o.Id)
	}

	if len(message.Gatekey) > 0 && message.Gatekey != o.Id {
		var sibling = discovery.GatewayGet(message.Gatekey)
		if sibling != nil {
			return sibling.Post(message, discovery)
		}
	}

	if message.Type&qtiny.MessageTypeReply > 0 {
		message.Address = message.Sender
		if message.Sender == o.Id {
			message.LocalFlag = message.LocalFlag | qtiny.MessageFlagLocalOnly
		}
	}

	message.Sender = o.Id

	if message.LocalFlag&qtiny.MessageFlagLocalOnly > 0 {
		return o.Post(message, discovery)
	}

	if message.LocalFlag&qtiny.MessageFlagRemoteOnly == 0 {
		var local, err = discovery.NanoLocalGet(message.Address)
		if err != nil {
			return err
		}
		if local != nil {
			message.LocalFlag = message.LocalFlag & qtiny.MessageFlagLocalOnly
			return o.Post(message, discovery)
		}
	}

	if o.Publisher == nil {
		return qerr.StackStringErr(0, message.GetTraceDepth(), "gateway %v publisher is not set", o.Id)
	}

	var data, err = message.ToJson()
	if err != nil {
		return err
	}

	if message.Type&qtiny.MessageTypeReply > 0 {
		var portal = discovery.PortalGet(message.Address)
		return o.Publisher(qtiny.MessageTypeReply, message.Address, portal, nil, message, discovery, o, data)
	}

	remote, err := discovery.NanoRemoteGet(message.Address)

	if err != nil {
		return err
	}

	if remote == nil {
		return qerr.StackStringErr(0, message.GetTraceDepth(), "%v discovery return nil remote", o.Id)
	}

	if message.Type&qtiny.MessageTypeBroadcast > 0 {
		var portalAddresses = remote.PortalAddresses()
		var portalCount = len(portalAddresses)
		for i := 0; i < portalCount; i++ {
			var portalAddress = portalAddresses[i]
			var portal = discovery.PortalGet(portalAddress)
			if portal.GetTypeHash() != o.GetTypeHash() {
				continue
			}
			_ = o.Publisher(qtiny.MessageTypeBroadcast, portalAddress, portal, remote, message, discovery, o, data)
		}
		return nil
	}

	if message.Type&qtiny.MessageTypeMulticast > 0 {
		return qerr.StackStringErr(0, message.GetTraceDepth(), "multicast is not implement")
	}

	var portalAddresses, pointer = remote.PortalPointer()
	if portalAddresses == nil {
		return qerr.StackStringErr(0, message.GetTraceDepth(), "%v portal addresses is empty for %v", o.Id, message.Address)
	}
	var published = false
	var portalCount = len(portalAddresses)
	for i := 0; i < portalCount; i++ {
		var portalAddress = portalAddresses[pointer]
		var portal = discovery.PortalGet(portalAddress)
		if portal != nil && portal.GetTypeHash() == o.GetTypeHash() {
			err = o.Publisher(qtiny.MessageTypeSend, portalAddress, portal, remote, message, discovery, o, data)
			if err == nil {
				published = true
				break
			}
		}
		pointer++
		if pointer >= portalCount {
			pointer = 0
		}

	}
	if err == nil && !published {
		err = qerr.StackStringErr(0, message.GetTraceDepth(),
			"cannot find any possible portal for gateway type %v [%v.%v]", o.GetType(), o.GetNodeId(), o.GetId())
	}
	return err
}

/* ============================================================================================= */

func (o *MemGateway) GetId() string {
	return o.Id
}

func (o *MemGateway) SetId(id string) {
	o.Id = id
}

func (o *MemGateway) GetNodeId() string {
	return o.NodeId
}

func (o *MemGateway) SetNodeId(nodeId string) {
	o.NodeId = nodeId
}

func (o *MemGateway) GetTag() string {
	return o.Tag
}

func (o *MemGateway) GetLogger() *log.Logger {
	return o.Logger
}

func (o *MemGateway) SetLogger(logger *log.Logger) {
	o.Logger = logger
}

func (o *MemGateway) GetType() string {
	if len(o.Type) == 0 {
		o.Type = "memory"
	}
	return o.Type
}

func (o *MemGateway) GetTypeHash() uint32 {
	if o.TypeHash == 0 {
		var hash = fnv.New32a()
		var _, err = hash.Write([]byte(o.GetType()))
		if err != nil {
			panic(err)
		}
		o.TypeHash = hash.Sum32()
	}
	return o.TypeHash
}

func (o *MemGateway) GetMeta() map[string]interface{} {
	if o.Meta == nil {
		o.Meta = make(map[string]interface{})
		o.Meta["id"] = o.Id
		o.Meta["type"] = o.GetType()
	}
	return o.Meta
}

func (o *MemGateway) EventChannelSend(event qtiny.GatewayEvent, meta map[string]interface{}) {
	if o.EventChannels == nil {
		return
	}

	o.EventChannelsMutex.RLock()
	defer o.EventChannelsMutex.RUnlock()

	var box = &qtiny.GatewayEventBox{
		Event:  event,
		Meta:   meta,
		Source: o,
	}
	for _, ch := range o.EventChannels {
		ch <- box
	}
}

func (o *MemGateway) EventChannelGet(channelId string) (<-chan *qtiny.GatewayEventBox, error) {

	o.EventChannelsMutex.Lock()
	defer o.EventChannelsMutex.Unlock()

	if o.EventChannels == nil {
		o.EventChannels = make(map[string]chan *qtiny.GatewayEventBox)
	}

	var ch = o.EventChannels[channelId]
	if ch == nil {
		ch = make(chan *qtiny.GatewayEventBox, 16)
		o.EventChannels[channelId] = ch
	}
	return ch, nil
}

func (o *MemGateway) EventChannelClose(channelId string) error {
	o.EventChannelsMutex.Lock()
	defer o.EventChannelsMutex.Unlock()

	if o.EventChannels == nil {
		return nil
	}

	var ch = o.EventChannels[channelId]
	if ch == nil {
		return nil
	}

	delete(o.EventChannels, channelId)
	close(ch)
	return nil
}

func (o *MemGateway) GetConfig() map[string]interface{} {
	return o.Config
}

func (o *MemGateway) SetConfig(config map[string]interface{}) {
	o.Config = config
}

func (o *MemGateway) GetIdHash() uint32 {
	if o.IdHash == 0 && len(o.Id) > 0 {
		var hash = fnv.New32a()
		var _, err = hash.Write([]byte(o.Id))
		if err != nil {
			panic(err)
		}
		o.IdHash = hash.Sum32()
	}
	return o.IdHash
}

func (o *MemGateway) GetNodeIdHash() uint32 {
	if o.NodeIdHash == 0 && len(o.NodeId) > 0 {
		var hash = fnv.New32a()
		var _, err = hash.Write([]byte(o.NodeId))
		if err != nil {
			panic(err)
		}
		o.NodeIdHash = hash.Sum32()
	}
	return o.NodeIdHash
}
