package zookeeper

import (
	"fmt"
	"github.com/camsiabor/go-zookeeper/zk"
	"github.com/camsiabor/qservice/impl/memory"
	"github.com/camsiabor/qservice/qtiny"
	"github.com/twinj/uuid"
	"os"
	"sync"
)

type ZooGateway struct {
	memory.MemGateway
	watcher *ZooWatcher

	connectId     string
	pathNodeQueue string

	consumeSemaphore sync.WaitGroup
}

const PathNodeQueue = "/qnode"

func (o *ZooGateway) Init(config map[string]interface{}) error {

	var err error

	if o.watcher == nil {
		o.watcher = &ZooWatcher{}
		o.watcher.Logger = o.Logger
	}

	err = o.watcher.Start(config)

	if err != nil {
		return err
	}

	o.pathNodeQueue = fmt.Sprintf("%s/%s", PathNodeQueue, o.GetId())
	o.watcher.AddConnectCallback(o.handleConnectionEvents)

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
		if o.Logger != nil {
			o.Logger.Println("zookeeper gateway disconnected ", o.watcher.Endpoints)
		}
		return
	}

	if event.State == zk.StateConnected || event.State == zk.StateConnectedReadOnly {
		if o.Logger != nil {
			o.Logger.Println("zookeeper gateway connected ", o.watcher.Endpoints)
		}

		if len(o.connectId) == 0 {
			var hostname, _ = os.Hostname()
			o.connectId = hostname + ":" + uuid.NewV4().String()
		}

		_, _ = watcher.Create(PathNano, []byte(""), 0, zk.WorldACL(zk.PermAll))
		_, _ = watcher.Create(o.pathNodeQueue, []byte(""), 0, zk.WorldACL(zk.PermAll))

		o.watcher.Watch(WatchTypeChildren, o.pathNodeQueue, o.pathNodeQueue, o.nodeQueueConsume)

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

func (o *ZooGateway) publish(consumerAddress string, prefix string, data []byte) error {
	var uri = o.GetQueueZNodePath(consumerAddress)
	var _, err = o.watcher.GetConn().Create(uri+prefix, data, zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil && o.Logger != nil {
		o.Logger.Println("publish error ", err.Error())
	}
	return err
}

func (o *ZooGateway) Post(message *qtiny.Message) error {

	if o.Queue == nil {
		return fmt.Errorf("gateway not started yet")
	}

	if message.Type&qtiny.MessageTypeReply > 0 {
		message.Address = message.Sender
	}

	message.Sender = o.GetId()

	if message.Flag&qtiny.MessageFlagLocalOnly > 0 {
		return o.MemGateway.Post(message)
	}

	if message.Flag&qtiny.MessageFlagRemoteOnly == 0 {
		var local, err = o.Discovery.NanoLocalGet(message.Address)
		if err != nil {
			return err
		}
		if local != nil {
			return o.MemGateway.Post(message)
		}
	}

	var data, err = message.ToJson()
	if err != nil {
		return err
	}

	if message.Type&qtiny.MessageTypeReply > 0 {
		return o.publish(message.Address, "/r", data)
	}
	remote, err := o.Discovery.NanoRemoteGet(message.Address)
	if err != nil {
		return err
	}
	if remote == nil {
		return fmt.Errorf("discovery return nil remote : %v", o.Discovery)
	}

	if message.Type&qtiny.MessageTypeBroadcast > 0 {
		var consumerAddresses = remote.RemoteAddresses()
		for i := 0; i < len(consumerAddresses); i++ {
			var consumerAddress = consumerAddresses[i]
			var perr = o.publish(consumerAddress, "/b", data)
			if perr != nil {
				err = perr
			}
		}
	} else {
		var consumerAddress = remote.RemoteAddress(-1)
		err = o.publish(consumerAddress, "/p", data)
	}
	return err
}

func (o *ZooGateway) Broadcast(message *qtiny.Message) error {
	message.Type = message.Type | qtiny.MessageTypeBroadcast
	return o.Post(message)
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
	return PathNodeQueue + "/" + nodeId
}
