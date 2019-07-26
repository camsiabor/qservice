package udp

import (
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/impl/memory"
	"net"
)

import (
	"fmt"
	"github.com/camsiabor/qservice/qtiny"
	"github.com/twinj/uuid"
	"golang.org/x/net/context"
	"os"
)

type UdpGateway struct {
	memory.MemGateway

	connectId string

	port int
	host string

	listener *net.UDPConn
	conns    map[string]*net.UDPConn
}

const PathNodeQueue = "/qnode"
const PathConnection = "/qconn"

func (o *UdpGateway) Init(config map[string]interface{}) error {

	var err error

	o.host = util.GetStr(config, "", "host")
	o.port = util.GetInt(config, 9981, "port")

	o.listener, err = net.ListenUDP("udp", &net.UDPAddr{
		IP:   net.ParseIP("0.0.0.0"),
		Port: o.port,
	})

	return err
}

func (o *UdpGateway) Start(config map[string]interface{}) error {
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

func (o *UdpGateway) Stop(config map[string]interface{}) error {

	if o.watcher != nil {
		var err = o.watcher.Stop(config)
		if err != nil {
			return err
		}
	}

	return o.MemGateway.Stop(config)
}

func (o *UdpGateway) handleEvent(event EtcdWatcherEvent, watcher *EtcdWatcher, err error) {

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

func (o *UdpGateway) Poll(limit int) (chan *qtiny.Message, error) {

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

func (o *UdpGateway) Post(message *qtiny.Message) error {

	if o.Queue == nil {
		return fmt.Errorf("gateway not started yet")
	}

	if message.Type&qtiny.MessageTypeReply > 0 {
		message.Address = message.Sender
	}

	message.Sender = o.GetNodeId()

	if message.LocalFlag&qtiny.MessageFlagLocalOnly > 0 {
		return o.MemGateway.Post(message)
	}

	if message.LocalFlag&qtiny.MessageFlagRemoteOnly == 0 {
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

func (o *UdpGateway) Broadcast(message *qtiny.Message) error {
	message.Type = message.Type | qtiny.MessageTypeBroadcast
	return o.Post(message)
}

/* ======================== producer ======================================== */

func (o *UdpGateway) publish(consumerAddress string, prefix string, data []byte) error {

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

func (o *UdpGateway) nodeQueueConsume() {
	for o.Looping {
		data, err := o.localQueue.Dequeue()
		if err != nil {
			o.Logger.Printf("%v node queue consume error %v", o.String(), err.Error())
		}
		go o.messageConsume([]byte(data))
	}
}

func (o *UdpGateway) messageConsume(data []byte) {
	var msg = &qtiny.Message{}
	_ = msg.FromJson(data)
	msg.Timeout = 0
	o.Queue <- msg
}

func (o *UdpGateway) String() string {
	return fmt.Sprintf("[etcd gateway - %v]", o.GetId())
}

func (o *UdpGateway) GetType() string {
	return "udp"
}

func (o *UdpGateway) GetMeta() map[string]interface{} {
	if o.Meta == nil {
		o.Meta = make(map[string]interface{})
		o.Meta["id"] = o.GetId()
		o.Meta["type"] = o.GetType()
		o.Meta["port"] = o.port
		o.Meta["host"] = o.host
	}
	return o.Meta
}
