package etcd

import (
	"fmt"
	"github.com/camsiabor/qservice/impl/memory"
	"github.com/camsiabor/qservice/qtiny"
	"github.com/twinj/uuid"
	"golang.org/x/net/context"
	"os"
)

type EtcdGateway struct {
	memory.MemGateway
	watcher *EtcdWatcher

	connectId          string
	pathNodeQueue      string
	pathNodeConnection string

	localQueue *Queue

	remoteQueues map[string]*Queue
}

const PathNodeQueue = "/qnode"
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

	if o.remoteQueues == nil {
		o.remoteQueues = make(map[string]*Queue)
	}

	err = o.watcher.Start(config)

	if err != nil {
		return err
	}

	o.watcher.AddConnectCallback(o.handleEvent)

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

	if o.watcher != nil {
		var err = o.watcher.Stop(config)
		if err != nil {
			return err
		}
	}

	return o.MemGateway.Stop(config)
}

func (o *EtcdGateway) handleEvent(event EtcdWatcherEvent, watcher *EtcdWatcher, err error) {

	var tag string
	if event == EtcdWatcherEventConnected {
		tag = fmt.Sprintf("connected %v", o.watcher.Endpoints)
	} else if event == EtcdWatcherEventDisconnected {
		tag = fmt.Sprintf("disconnected %v", o.watcher.Endpoints)
	}

	if err != nil {
		o.Logger.Println(tag, err.Error())
		return
	}

	if o.Logger != nil {
		o.Logger.Printf("etcd gateway %v ", tag)
	}

	if event == EtcdWatcherEventDisconnected {
		return
	}

	if len(o.connectId) == 0 {
		var hostname, _ = os.Hostname()
		o.connectId = hostname + ":" + uuid.NewV4().String()
	}

	_, _ = watcher.Create(PathNodeQueue, "", 0)
	_, _ = watcher.Create(PathConnection, "", 0)

	_, _ = watcher.Create(o.pathNodeQueue, "", 0)
	_, _ = watcher.Create(o.pathNodeConnection, "", 0)
	_, _ = watcher.Create(o.pathNodeConnection+"/"+o.connectId, "", 0)

	if o.localQueue == nil {
		var ctx, cancel = context.WithCancel(context.TODO())
		o.localQueue = &Queue{
			ctx:        ctx,
			keyPrefix:  o.pathNodeQueue,
			client:     o.watcher.GetConn(),
			cancelFunc: cancel,
		}
		go o.nodeQueueConsume()
	}

	if o.Logger != nil {
		o.Logger.Println("etcd gateway connected setup fin")
	}

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
		var local, err = o.Discovery.NanoLocalGet(message.Address)
		if err != nil {
			return err
		}
		if local != nil {
			return o.MemGateway.Post(message)
		}
	}

	data, err := message.ToJson()
	if err != nil {
		return err
	}

	if message.Type&qtiny.MessageTypeReply > 0 {
		return o.publish(message.Address, "/r", data)
	}

	nano, err := o.Discovery.NanoRemoteGet(message.Address)
	if err != nil {
		return err
	}

	if nano == nil {
		o.Logger.Printf("discovery return nil nano %v", o.Discovery)
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

/* ======================== producer ======================================== */

func (o *EtcdGateway) publish(consumerAddress string, prefix string, data []byte) error {

	var remoteQueue = o.remoteQueues[consumerAddress]
	if remoteQueue == nil {
		var ctx, cancel = context.WithCancel(context.TODO())
		remoteQueue = &Queue{
			ctx:        ctx,
			cancelFunc: cancel,
			client:     o.watcher.GetConn(),
			keyPrefix:  PathNodeQueue + "/" + consumerAddress,
		}
		func() {
			o.Mutex.Lock()
			defer o.Mutex.Unlock()
			o.remoteQueues[consumerAddress] = remoteQueue
		}()
	}

	return remoteQueue.Enqueue(string(data))
}

/* ======================== consumer loop ========================================== */

func (o *EtcdGateway) nodeQueueConsume() {
	for o.Looping {
		data, err := o.localQueue.Dequeue()
		if err != nil {
			o.Logger.Printf("%v node queue consume error %v", o.String(), err.Error())
		}
		go o.messageConsume([]byte(data))
	}
}

func (o *EtcdGateway) messageConsume(data []byte) {
	var msg = &qtiny.Message{}
	_ = msg.FromJson(data)
	msg.Timeout = 0
	o.Queue <- msg
}

func (o *EtcdGateway) String() string {
	return fmt.Sprintf("[etcd gateway - %v]", o.GetId())
}
