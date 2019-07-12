package zookeeper

import (
	"fmt"
	"github.com/camsiabor/go-zookeeper/zk"
	"github.com/camsiabor/qservice/impl/memory"
	"github.com/camsiabor/qservice/qtiny"
)

type ZGateway struct {
	memory.MGateway
	watcher *ZooWatcher

	pathNodeQueue string
}

const PathNodeQueue = "/qnode"
const PathService = "/qservice"

func (o *ZGateway) Init(config map[string]interface{}) error {

	var err error

	if o.watcher == nil {
		o.watcher = &ZooWatcher{}
	}

	err = o.watcher.Start(config)

	if err != nil {
		return err
	}

	o.pathNodeQueue = fmt.Sprintf("%s/%s", PathNodeQueue, o.GetId())
	o.watcher.AddConnectCallback(func(event *zk.Event, watcher *ZooWatcher, err error) {
		if event.State == zk.StateConnected || event.State == zk.StateConnectedReadOnly {
			// TODO exception
			_ = watcher.Create(PathService, []byte(""), 0, zk.WorldACL(zk.PermAll))
			_ = watcher.Create(PathNodeQueue, []byte(""), 0, zk.WorldACL(zk.PermAll))
			_ = watcher.Create(o.pathNodeQueue, []byte(""), 0, zk.WorldACL(zk.PermAll))
			o.watcher.Watch(WatchTypeChildren, o.pathNodeQueue, o.pathNodeQueue, o.nodeQueueConsume)

			if o.Subscribers != nil {
				o.SubscriberMutex.RLock()
				for _, subscriber := range o.Subscribers {
					go o.serviceCreateRegistry(subscriber.Address, subscriber.Options)
				}
				o.SubscriberMutex.RUnlock()
			}

		}
	})

	return nil
}

func (o *ZGateway) Start(config map[string]interface{}) error {
	var err error
	defer func() {
		if err != nil {
			_ = o.Stop(config)
		}
	}()
	err = o.MGateway.Start(config)
	if err == nil {
		err = o.Init(config)
	}
	return err
}

func (o *ZGateway) Stop(config map[string]interface{}) error {
	if o.watcher != nil {
		var err = o.watcher.Stop(config)
		if err != nil {
			return err
		}
	}
	return o.MGateway.Stop(config)
}

func (o *ZGateway) Loop() {
	var ok bool
	var msg *qtiny.Message
	for {
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

}

func (o *ZGateway) Poll(limit int) (chan *qtiny.Message, error) {

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

func (o *ZGateway) publish(consumerAddress string, prefix string, data []byte) error {
	var uri = o.GetQueueZNodePath(consumerAddress)
	var _, err = o.watcher.GetConn().Create(uri+prefix, data, zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	return err
}

func (o *ZGateway) Post(message *qtiny.Message) error {
	if o.Queue == nil {
		return fmt.Errorf("gateway not started yet")
	}
	var data, err = message.ToJson()
	if err != nil {
		return err
	}

	var service = o.ServiceRemoteGet(message.Address)
	if service == nil || service.NodeAddresses() == nil {
		service = o.ServiceRemoteNew(message.Address)
		var serviceZNodePath = o.GetServiceZNodePath(message.Address)
		var children, _, err = o.watcher.GetConn().Children(serviceZNodePath)
		if err != nil {
			return err
		}
		o.watcher.Watch(WatchTypeChildren, serviceZNodePath, serviceZNodePath, o.serviceRegistryWatch)
		if children == nil || len(children) == 0 {
			return fmt.Errorf("no consumer found for " + message.Address)
		}
		for i := 0; i < len(children); i++ {
			service.NodeAdd(children[i], children[i])
		}
	}
	if message.Type&qtiny.MessageTypeBroadcast > 0 {
		var consumerAddresses = service.NodeAddresses()
		for i := 0; i < len(consumerAddresses); i++ {
			var consumerAddress = consumerAddresses[i]
			var perr = o.publish(consumerAddress, "/b", data)
			if perr != nil {
				err = perr
			}
		}
	} else {
		var consumerAddress = service.NodeAddress(-1)
		err = o.publish(consumerAddress, "/p", data)
	}
	return err
}

func (o *ZGateway) Broadcast(message *qtiny.Message) error {
	message.Type = message.Type | qtiny.MessageTypeBroadcast
	return o.Post(message)
}

func (o *ZGateway) serviceCreateRegistry(address string, options qtiny.ServiceOptions) error {
	<-o.watcher.WaitForConnected()

	var parent = o.GetServiceZNodePath(address)
	var err = o.watcher.Create(parent, []byte(""), 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}

	var path = o.GetServiceZNodeSelfPath(address)
	err = o.watcher.Create(path, []byte(""), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	return err
}

func (o *ZGateway) ServiceRegister(address string, options qtiny.ServiceOptions) error {
	o.SubscriberAdd(address, options)
	go o.serviceCreateRegistry(address, options)
	return nil
}

func (o *ZGateway) ServiceUnregister(address string) error {
	// TODO implementation
	return nil
}

func (o *ZGateway) serviceRegistryWatch(event *zk.Event, stat *zk.Stat, data interface{}, box *WatchBox, watcher *ZooWatcher, err error) bool {
	return true
}

func (o *ZGateway) nodeQueueConsume(event *zk.Event, stat *zk.Stat, data interface{}, box *WatchBox, watcher *ZooWatcher, err error) bool {
	if data == nil {
		return true
	}
	var children, ok = data.([]string)
	if !ok {
		return true
	}
	var n = len(children)
	var root = box.GetPath()
	var conn = o.watcher.GetConn()
	for i := 0; i < n; i++ {
		var path = fmt.Sprintf("%s/%s", root, children[i])
		var data, stat, err = conn.Get(path)
		if err != nil {
			continue
		}
		go o.messageConsume(data)
		err = conn.Delete(path, stat.Version)
	}
	return true
}

func (o *ZGateway) messageConsume(data []byte) {
	// TODO error handling
	var msg = &qtiny.Message{}
	_ = msg.FromJson(data)
	o.Queue <- msg
}

func (o *ZGateway) GetQueueZNodePath(nodeId string) string {
	return PathNodeQueue + "/" + nodeId
}

func (o *ZGateway) GetServiceZNodePath(address string) string {
	return PathService + "/" + address
}

func (o *ZGateway) GetServiceZNodeSelfPath(address string) string {
	return fmt.Sprintf("%s/%s/%s:%s", PathService, address, o.GetId(), o.GetTag())
}
