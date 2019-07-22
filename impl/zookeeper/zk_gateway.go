package zookeeper

import (
	"encoding/json"
	"fmt"
	"github.com/camsiabor/go-zookeeper/zk"
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/impl/memory"
	"github.com/camsiabor/qservice/qtiny"
	"os"
	"sync"
	"time"
)

type ZooGateway struct {
	memory.MemGateway
	watcher *ZooWatcher

	pathRootQueue string
	pathNodeQueue string

	consumeSemaphore sync.WaitGroup
}

func (o *ZooGateway) Init(config map[string]interface{}) error {

	var err error

	if len(o.pathRootQueue) == 0 {
		o.pathRootQueue = util.GetStr(config, "/qqueue", "queue.root")
	}

	if o.watcher == nil {
		o.watcher = &ZooWatcher{}
		o.watcher.Logger = o.Logger
	}

	err = o.watcher.Start(config)

	o.GetMeta()

	if err != nil {
		return err
	}

	o.watcher.AddCallback(o.handleConnectionEvents)

	return err
}

func (o *ZooGateway) Start(config map[string]interface{}) error {
	var err error
	defer func() {
		if err != nil {
			_ = o.Stop(config)
		}
	}()
	err = o.MemGateway.Start(config)
	if err == nil {
		err = o.Init(config)
	}
	return err
}

func (o *ZooGateway) Stop(config map[string]interface{}) error {

	if o.watcher != nil {
		var err = o.watcher.Stop(config)
		if err != nil {
			return err
		}
	}

	return o.MemGateway.Stop(config)
}

func (o *ZooGateway) handleConnectionEvents(event *zk.Event, watcher *ZooWatcher, err error) {

	if event.State == zk.StateDisconnected {

		o.EventChannelSend(qtiny.GatewayEventDisconnected, o.GetMeta())

		if o.Logger != nil {
			o.Logger.Println("zookeeper gateway disconnected ", o.watcher.Endpoints)
		}
		return
	}

	if event.State == zk.StateConnected || event.State == zk.StateConnectedReadOnly {

		o.EventChannelSend(qtiny.GatewayEventConnected, o.GetMeta())

		if o.Logger != nil {
			o.Logger.Println("zookeeper gateway connected ", o.watcher.Endpoints)
		}

		_, err = o.watcher.Create(o.pathRootQueue, []byte(""), 0, zk.WorldACL(zk.PermAll), false)
		if err != nil {
			o.Logger.Printf("%v create queue root error %v", o.GetId(), err.Error())
		}

		o.pathNodeQueue = fmt.Sprintf("%v/%v", o.pathRootQueue, o.GetId())
		var data, _ = json.Marshal(o.GetMeta())
		_, err = o.watcher.Create(o.pathNodeQueue, data, 0, zk.WorldACL(zk.PermAll), true)
		if err != nil {
			o.Logger.Printf("%v create queue error %v", o.GetId(), err.Error())
		}

		o.watcher.Watch(WatchTypeChildren, o.pathNodeQueue, nil, time.Hour, o.nodeQueueConsume)

		if o.Logger != nil {
			o.Logger.Println("zookeeper gateway connected setup fin", o.GetId())
		}

	}
}

func (o *ZooGateway) loop() {
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

func (o *ZooGateway) Poll(limit int) (chan *qtiny.Message, error) {

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

func (o *ZooGateway) publish(
	portalAddress string, message *qtiny.Message,
	prefix string, data []byte,
	discovery qtiny.Discovery, remote *qtiny.Nano, retry int) error {

	var portal = discovery.PortalGet(portalAddress)

	if portal == nil || len(portal.GetType()) == 0 {
		if remote != nil && retry > 0 {
			portalAddress = remote.PortalAddress(-1)
			return o.publish(portalAddress, message, prefix, data, discovery, remote, retry-1)
		}
		return fmt.Errorf("route %v to portal address %v but no portal found", message.Address, portalAddress)
	}

	var uri = o.GetQueueZNodePath(portalAddress)
	var _, err = o.watcher.GetConn().Create(uri+prefix, data, zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil && o.Logger != nil {
		o.Logger.Println("publish error ", err.Error())
	}
	return err
}

func (o *ZooGateway) Post(message *qtiny.Message, discovery qtiny.Discovery) error {

	if o.Queue == nil {
		return fmt.Errorf("gateway not started yet")
	}

	if message.Type&qtiny.MessageTypeReply > 0 {
		message.Address = message.Sender
		if message.Sender == o.GetId() {
			message.Flag = message.Flag | qtiny.MessageFlagLocalOnly
		}
	}

	message.Sender = o.GetId()

	if message.Flag&qtiny.MessageFlagLocalOnly > 0 {
		return o.MemGateway.Post(message, discovery)
	}

	if message.Flag&qtiny.MessageFlagRemoteOnly == 0 {
		var local, err = discovery.NanoLocalGet(message.Address)
		if err != nil {
			return err
		}
		if local != nil {
			message.Flag = message.Flag & qtiny.MessageFlagLocalOnly
			return o.MemGateway.Post(message, discovery)
		}
	}

	var data, err = message.ToJson()
	if err != nil {
		return err
	}

	if message.Type&qtiny.MessageTypeReply > 0 {
		return o.publish(message.Address, message, "/r", data, discovery, nil, 0)
	}
	remote, err := discovery.NanoRemoteGet(message.Address)
	if err != nil {
		return err
	}
	if remote == nil {
		return fmt.Errorf("discovery return nil remote : %v", discovery)
	}

	if message.Type&qtiny.MessageTypeBroadcast > 0 {
		var portalAddresses = remote.PortalAddresses()
		for i := 0; i < len(portalAddresses); i++ {
			var consumerAddress = portalAddresses[i]
			var perr = o.publish(consumerAddress, message, "/b", data, discovery, nil, 0)
			if perr != nil {
				err = perr
			}
		}
	} else {
		var portalAddress = remote.PortalAddress(-1)
		err = o.publish(portalAddress, message, "/p", data, discovery, remote, 8)
	}
	return err
}

func (o *ZooGateway) Multicast(message *qtiny.Message, discovery qtiny.Discovery) error {
	message.Type = message.Type | qtiny.MessageTypeMulticast
	return o.Post(message, discovery)
}

func (o *ZooGateway) Broadcast(message *qtiny.Message, discovery qtiny.Discovery) error {
	message.Type = message.Type | qtiny.MessageTypeBroadcast
	return o.Post(message, discovery)
}

func (o *ZooGateway) nodeQueueConsume(event *zk.Event, stat *zk.Stat, data interface{}, box *WatchBox, watcher *ZooWatcher, err error) bool {
	if err != nil {
		if o.Logger != nil {
			o.Logger.Println("node queue consume error", err.Error())
		}
		return true
	}
	if data == nil {
		return true
	}
	var children, ok = data.([]string)
	if !ok {
		return true
	}
	var n = len(children)
	if n == 0 {
		return true
	}
	var root = box.GetPath()
	var conn = o.watcher.GetConn()

	for i := 0; i < n; i++ {
		o.consumeSemaphore.Add(1)
		go o.messageConsume(conn, root, children[i])
	}
	o.consumeSemaphore.Wait()

	return true
}

func (o *ZooGateway) messageConsume(conn *zk.Conn, root string, child string) {
	defer o.consumeSemaphore.Done()
	var path = root + "/" + child
	var data, stat, err = conn.Get(path)
	if err != nil {
		return
	}
	var msg = &qtiny.Message{}
	_ = msg.FromJson(data)
	msg.Timeout = 0
	o.Queue <- msg
	_ = conn.Delete(path, stat.Version)
}

func (o *ZooGateway) GetQueueZNodePath(nodeId string) string {
	return o.pathRootQueue + "/" + nodeId
}

func (o *ZooGateway) GetType() string {
	return "zookeeper"
}

func (o *ZooGateway) GetMeta() map[string]interface{} {
	if o.Meta == nil {
		var hostname, _ = os.Hostname()
		o.Meta = make(map[string]interface{})
		o.Meta["id"] = o.GetId()
		o.Meta["type"] = o.GetType()
		o.Meta["hostname"] = hostname
	}
	if o.Meta["endpoints"] == nil {
		o.Meta["endpoints"] = o.watcher.Endpoints
	}
	return o.Meta
}
