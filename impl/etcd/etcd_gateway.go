package etcd

import (
	"fmt"
	"github.com/camsiabor/go-zookeeper/zk"
	"github.com/camsiabor/qcom/qroutine"
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/impl/memory"
	"github.com/camsiabor/qservice/qtiny"
	"github.com/twinj/uuid"
	"go.etcd.io/etcd/clientv3"
	"golang.org/x/net/context"
	"os"
	"sync"
	"time"
)

type EtcdGateway struct {
	memory.MemGateway
	watcher *EtcdWatcher

	connectId          string
	pathNodeQueue      string
	pathNodeConnection string

	consumeSemaphore sync.WaitGroup

	timer *qroutine.Timer
}

const PathNodeQueue = "/qnode"
const PathNano = "/qnano"
const PathConnection = "/qconn"

func (o *EtcdGateway) Init(config map[string]interface{}) error {

	var err error

	o.pathNodeQueue = fmt.Sprintf("%s/%s", PathNodeQueue, o.GetId())
	o.pathNodeConnection = fmt.Sprintf("%s/%s", PathConnection, o.GetId())

	if o.watcher == nil {
		o.watcher = &EtcdWatcher{}
		o.watcher.Logger = o.Logger
		o.watcher.HeartbeatPath = o.pathNodeConnection
	}

	err = o.watcher.Start(config)

	if err != nil {
		return err
	}

	o.watcher.AddConnectCallback(o.handleConnectionEvents)

	if o.timer != nil {
		o.timer.Stop()
	}

	var scanInterval = util.GetInt(config, 3*60, "scan.interval")

	o.timer = &qroutine.Timer{}
	err = o.timer.Start(0, time.Duration(scanInterval)*time.Second, o.timerloop)
	return err
}

func (o *EtcdGateway) Start(config map[string]interface{}) error {
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

func (o *EtcdGateway) Stop(config map[string]interface{}) error {

	if o.timer != nil {
		o.timer.Stop()
	}

	if o.watcher != nil {
		var err = o.watcher.Stop(config)
		if err != nil {
			return err
		}
	}

	return o.MemGateway.Stop(config)
}

func (o *EtcdGateway) handleConnectionEvents(event *zk.Event, watcher *EtcdWatcher, err error) {

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

		_, _ = watcher.Create(PathNano, "", 0)
		_, _ = watcher.Create(PathNodeQueue, "", 0)
		_, _ = watcher.Create(PathConnection, "", 0)

		_, _ = watcher.Create(o.pathNodeQueue, "", 0)
		_, _ = watcher.Create(o.pathNodeConnection, "", 0)
		_, _ = watcher.Create(o.pathNodeConnection+"/"+o.connectId, "", 0)

		//o.watcher.Watch(zookeeper.WatchTypeChildren, o.pathNodeQueue, o.pathNodeQueue, o.nodeQueueConsume)

		go func() {
			for i := 0; i < 3; i++ {
				o.nanoLocalPublishRegistries()
				time.Sleep(o.watcher.SessionTimeout + time.Second)
			}
		}()

		o.timer.Wake()

		if o.Logger != nil {
			o.Logger.Println("zk gate connected setup fin")
		}

	}
}

func (o *EtcdGateway) loop() {
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

func (o *EtcdGateway) timerloop(timer *qroutine.Timer, err error) {
	if err != nil {
		return
	}
	var ch = o.watcher.WaitForConnected()
	if ch != nil {
		<-ch
	}
	o.nanoLocalPublishRegistries()
}

func (o *EtcdGateway) Poll(limit int) (chan *qtiny.Message, error) {

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

func (o *EtcdGateway) publish(consumerAddress string, prefix string, data []byte) error {
	var uri = o.GetQueueZNodePath(consumerAddress)
	var _, err = o.watcher.Create(uri+prefix, string(data), zk.FlagEphemeral|zk.FlagSequence)
	if err != nil && o.Logger != nil {
		o.Logger.Println("publish error ", err.Error())
	}
	return err
}

func (o *EtcdGateway) Post(message *qtiny.Message) error {

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
		var subscriber = o.Locals[message.Address]
		if subscriber != nil {
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

	var nano = o.RemoteGet(message.Address)
	if nano == nil || nano.RemoteAddresses() == nil {
		nano = o.NanoRemoteRegister(message.Address)
		var nanoZNodePath = o.GetNanoZNodePath(message.Address)
		fmt.Println(nanoZNodePath)
		// TODO
		//var children, _, err = o.watcher.GetConn().Children(nanoZNodePath)
		var children []string
		if err != nil {
			return fmt.Errorf("no consumer found : " + err.Error())
		}
		// TODO
		//o.watcher.Watch(zookeeper.WatchTypeChildren, nanoZNodePath, nanoZNodePath, o.nanoRemoteRegistryWatch)
		if children == nil || len(children) == 0 {
			return fmt.Errorf("no consumer found for " + message.Address)
		}
		for i := 0; i < len(children); i++ {
			nano.RemoteAdd(children[i], children[i])
		}
	}
	if message.Type&qtiny.MessageTypeBroadcast > 0 {
		var consumerAddresses = nano.RemoteAddresses()
		for i := 0; i < len(consumerAddresses); i++ {
			var consumerAddress = consumerAddresses[i]
			var perr = o.publish(consumerAddress, "/b", data)
			if perr != nil {
				err = perr
			}
		}
	} else {
		var consumerAddress = nano.RemoteAddress(-1)
		err = o.publish(consumerAddress, "/p", data)
	}
	return err
}

func (o *EtcdGateway) Broadcast(message *qtiny.Message) error {
	message.Type = message.Type | qtiny.MessageTypeBroadcast
	return o.Post(message)
}

func (o *EtcdGateway) nanoLocalPublishRegistry(nano *qtiny.Nano) error {
	var parent = o.GetNanoZNodePath(nano.Address)
	var _, err = o.watcher.Create(parent, "", 0)
	if err != nil {
		if o.Logger != nil {
			o.Logger.Println("nano register fail ", parent, " : ", err.Error())
		}
		return err
	}
	var path = o.GetNanoZNodeSelfPath(nano.Address)
	var exist, cerr = o.watcher.Create(path, "", zk.FlagEphemeral)
	if !exist && o.Logger != nil {
		if cerr == nil {
			o.Logger.Println("nano register ", path)
		} else {
			o.Logger.Println("nano register fail ", path, " : ", cerr.Error())
		}
	}
	return cerr
}

func (o *EtcdGateway) nanoLocalPublishRegistries() {
	if o.Locals == nil {
		return
	}
	o.LocalsMutex.RLock()
	defer o.LocalsMutex.RUnlock()
	for _, subscriber := range o.Locals {
		go o.nanoLocalPublishRegistry(subscriber)
	}
}

func (o *EtcdGateway) NanoLocalRegister(nano *qtiny.Nano) error {

	if nano.Flag&qtiny.NanoFlagLocalOnly > 0 {
		return o.MemGateway.NanoLocalRegister(nano)
	}

	o.LocalAdd(nano)
	if o.watcher.IsConnected() {
		go o.nanoLocalPublishRegistry(nano)
	}
	return nil
}

func (o *EtcdGateway) NanoLocalUnregister(nano *qtiny.Nano) error {
	// TODO implementation
	return nil
}

func (o *EtcdGateway) nanoRemoteRegistryWatch(event *zk.Event, stat *zk.Stat, data interface{}, box *WatchBox, watcher *EtcdWatcher, err error) bool {
	if err != nil && o.Logger != nil {
		o.Logger.Println("nano registry watch error", err.Error())
		return true
	}
	if data == nil {
		return true
	}
	var children, ok = data.([]string)
	if o.Logger != nil {
		o.Logger.Println("remote consumer changes", box.Path, children)
	}
	if !ok {
		return true
	}
	var address = box.Path
	var nano = o.RemoteGet(address)
	if nano == nil {
		nano = o.NanoRemoteRegister(address)
	}
	nano.RemoteSet(children, nil)
	return true
}

func (o *EtcdGateway) nodeQueueConsume(event *zk.Event, stat *zk.Stat, data interface{}, box *WatchBox, watcher *EtcdWatcher, err error) bool {
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

func (o *EtcdGateway) messageConsume(conn *clientv3.Client, root string, child string) {
	defer o.consumeSemaphore.Done()
	var path = root + "/" + child
	var data, err = conn.Get(context.TODO(), path)
	if err != nil {
		return
	}
	var msg = &qtiny.Message{}
	_ = msg.FromJson(data.Kvs[0].Value)
	msg.Timeout = 0
	o.Queue <- msg
	// TODO
	//_ = conn.Delete(path, stat.Version)
}

func (o *EtcdGateway) GetQueueZNodePath(nodeId string) string {
	return PathNodeQueue + "/" + nodeId
}

func (o *EtcdGateway) GetNanoZNodePath(address string) string {
	return PathNano + "/" + address
}

func (o *EtcdGateway) GetNanoZNodeSelfPath(address string) string {
	return fmt.Sprintf("%s/%s/%s", PathNano, address, o.GetId())
}
