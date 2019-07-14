package zookeeper

import (
	"fmt"
	"github.com/camsiabor/go-zookeeper/zk"
	"github.com/camsiabor/qcom/qroutine"
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/impl/memory"
	"github.com/camsiabor/qservice/qtiny"
	"github.com/twinj/uuid"
	"os"
	"sync"
	"time"
)

type ZGateway struct {
	memory.MGateway
	watcher *ZooWatcher

	connectId          string
	pathNodeQueue      string
	pathNodeConnection string

	consumeSemaphore sync.WaitGroup

	timer *qroutine.Timer
}

const PathNodeQueue = "/qnode"
const PathService = "/qservice"
const PathConnection = "/qconn"

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
	o.pathNodeConnection = fmt.Sprintf("%s/%s", PathConnection, o.GetId())
	o.watcher.AddConnectCallback(o.handleConnectionEvents)

	if o.timer != nil {
		o.timer.Stop()
	}

	var scanInterval = util.GetInt(config, 3*60, "scan.interval")

	o.timer = &qroutine.Timer{}
	err = o.timer.Start(0, time.Duration(scanInterval)*time.Second, o.timerloop)
	return err
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
	if o.timer != nil {
		o.timer.Stop()
	}
	if o.watcher != nil {
		var err = o.watcher.Stop(config)
		if err != nil {
			return err
		}
	}
	return o.MGateway.Stop(config)
}

func (o *ZGateway) handleConnectionEvents(event *zk.Event, watcher *ZooWatcher, err error) {

	if event.State == zk.StateDisconnected {
		if o.Logger != nil {
			o.Logger.Println("zk gateway disconnected ", o.watcher.Endpoints)
		}
		return
	}

	if event.State == zk.StateConnected || event.State == zk.StateConnectedReadOnly {
		if o.Logger != nil {
			o.Logger.Println("zk gateway connected ", o.watcher.Endpoints)
		}

		if len(o.connectId) == 0 {
			var hostname, _ = os.Hostname()
			o.connectId = hostname + ":" + uuid.NewV4().String()
		}

		_, _ = watcher.Create(PathService, []byte(""), 0, zk.WorldACL(zk.PermAll))
		_, _ = watcher.Create(PathNodeQueue, []byte(""), 0, zk.WorldACL(zk.PermAll))
		_, _ = watcher.Create(PathConnection, []byte(""), 0, zk.WorldACL(zk.PermAll))

		_, _ = watcher.Create(o.pathNodeQueue, []byte(""), 0, zk.WorldACL(zk.PermAll))
		_, _ = watcher.Create(o.pathNodeConnection, []byte(""), 0, zk.WorldACL(zk.PermAll))
		_, _ = watcher.Create(o.pathNodeConnection+"/"+o.connectId, []byte(""), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))

		o.watcher.Watch(WatchTypeChildren, o.pathNodeQueue, o.pathNodeQueue, o.nodeQueueConsume)

		go func() {
			for i := 0; i < 3; i++ {
				o.serviceCreateRegistries()
				time.Sleep(o.watcher.SessionTimeout + time.Second)
			}
		}()

		o.timer.Wake()

		if o.Logger != nil {
			o.Logger.Println("zk gate connected setup fin")
		}

	}
}

func (o *ZGateway) loop() {
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

func (o *ZGateway) timerloop(timer *qroutine.Timer, err error) {
	if err != nil {
		return
	}
	var ch = o.watcher.WaitForConnected()
	if ch != nil {
		<-ch
	}
	o.serviceCreateRegistries()
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

	if message.Type&qtiny.MessageTypeReply > 0 {
		message.Address = message.Sender
	}

	message.Sender = o.GetId()
	var data, err = message.ToJson()
	if err != nil {
		return err
	}

	if message.Type&qtiny.MessageTypeReply > 0 {
		return o.publish(message.Address, "/r", data)
	}

	var service = o.ServiceRemoteGet(message.Address)
	if service == nil || service.NodeAddresses() == nil {
		service = o.ServiceRemoteNew(message.Address)
		var serviceZNodePath = o.GetServiceZNodePath(message.Address)
		var children, _, err = o.watcher.GetConn().Children(serviceZNodePath)
		if err != nil {
			return fmt.Errorf("no consumer found : " + err.Error())
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
	var parent = o.GetServiceZNodePath(address)
	var _, err = o.watcher.Create(parent, []byte(""), 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		if o.Logger != nil {
			o.Logger.Println("service register fail ", parent, " : ", err.Error())
		}
		return err
	}
	var path = o.GetServiceZNodeSelfPath(address)
	var exist, cerr = o.watcher.Create(path, []byte(""), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if !exist && o.Logger != nil {
		if cerr == nil {
			o.Logger.Println("service register ", path)
		} else {
			o.Logger.Println("service register fail ", path, " : ", cerr.Error())
		}
	}
	return cerr
}

func (o *ZGateway) serviceCreateRegistries() {
	if o.Subscribers == nil {
		return
	}
	o.SubscriberMutex.RLock()
	defer o.SubscriberMutex.RUnlock()
	for _, subscriber := range o.Subscribers {
		go o.serviceCreateRegistry(subscriber.Address, subscriber.Options)
	}
}

func (o *ZGateway) ServiceRegister(address string, options qtiny.ServiceOptions) error {
	o.SubscriberAdd(address, options)
	if o.watcher.IsConnected() {
		go o.serviceCreateRegistry(address, options)
	}
	return nil
}

func (o *ZGateway) ServiceUnregister(address string) error {
	// TODO implementation
	return nil
}

func (o *ZGateway) serviceRegistryWatch(event *zk.Event, stat *zk.Stat, data interface{}, box *WatchBox, watcher *ZooWatcher, err error) bool {
	if err != nil && o.Logger != nil {
		o.Logger.Println("service registry watch error", err.Error())
		return true
	}
	if data == nil {
		return true
	}
	var children, ok = data.([]string)
	if o.Logger != nil {
		o.Logger.Println("remote consumer changes", box.path, children)
	}
	if !ok {
		return true
	}
	var address = box.path
	var service = o.ServiceRemoteGet(address)
	if service == nil {
		return true
	}
	service.NodeSet(children, nil)
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

func (o *ZGateway) messageConsume(conn *zk.Conn, root string, child string) {
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

func (o *ZGateway) GetQueueZNodePath(nodeId string) string {
	return PathNodeQueue + "/" + nodeId
}

func (o *ZGateway) GetServiceZNodePath(address string) string {
	return PathService + "/" + address
}

func (o *ZGateway) GetServiceZNodeSelfPath(address string) string {
	return fmt.Sprintf("%s/%s/%s", PathService, address, o.GetId())
}
