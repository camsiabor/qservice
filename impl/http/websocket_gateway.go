package http

import (
	"fmt"
	"github.com/camsiabor/go-zookeeper/zk"
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/impl/memory"
	"github.com/camsiabor/qservice/qtiny"
	"github.com/gorilla/websocket"
	"net/http"
	"os"
	"sync"
	"time"
)

type WebsocketGateway struct {
	memory.MemGateway

	endpoint string

	wsserver *http.Server

	wsclientsMutex sync.RWMutex
	wsclients      map[string]*wsclient

	wsupgrader *websocket.Upgrader
}

type wsclient struct {
	id       string
	conn     *websocket.Conn
	request  *http.Request
	timeConn time.Time
}

func (o *WebsocketGateway) Init(config map[string]interface{}) error {

	var err error

	o.GetMeta()

	o.endpoint = util.GetStr(config, ":8080", "endpoint")

	if o.wsupgrader == nil {
		o.wsupgrader = &websocket.Upgrader{}
	}

	if o.wsserver == nil {
		o.wsserver = &http.Server{
			Addr:    o.endpoint,
			Handler: o,
		}
		err = o.wsserver.ListenAndServe()
		if err == nil {
			o.Logger.Printf("websocket gateway listening to %v", o.endpoint)
		} else {
			o.Logger.Printf("websocket gateway listen & serve %v error %v ", o.endpoint, err.Error())
		}
	}

	return err
}

func (o *WebsocketGateway) Start(config map[string]interface{}) error {
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

func (o *WebsocketGateway) Stop(config map[string]interface{}) error {

	if o.wsserver != nil {
		_ = o.wsserver.Close()
		o.wsserver = nil
		o.Logger.Printf("websocket gateway close")
	}

	return o.MemGateway.Stop(config)
}

func (o *WebsocketGateway) ServeHTTP(response http.ResponseWriter, request *http.Request) {

	if request.Proto != "ws" && request.Proto != "wss" {
		return
	}

	var conn, err = o.wsupgrader.Upgrade(response, request, nil)
	if err != nil {
		o.Logger.Printf("update request to ws connection %v fail %v", request.RemoteAddr, err.Error())
		return
	}

	var client = &wsclient{
		conn:     conn,
		request:  request,
		timeConn: time.Now(),
	}
	client.id = fmt.Sprintf("%v:%v", request.RemoteAddr, time.Now().Nanosecond())

	func() {
		o.wsclientsMutex.Lock()
		defer o.wsclientsMutex.Unlock()
		if o.wsclients == nil {
			o.wsclients = make(map[string]*wsclient)
		}
		o.wsclients[client.id] = client
	}()

}

func (o *WebsocketGateway) publish(
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

	// TODO publish

	return nil
}

func (o *WebsocketGateway) Post(message *qtiny.Message, discovery qtiny.Discovery) error {

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

func (o *WebsocketGateway) Multicast(message *qtiny.Message, discovery qtiny.Discovery) error {
	message.Type = message.Type | qtiny.MessageTypeMulticast
	return o.Post(message, discovery)
}

func (o *WebsocketGateway) Broadcast(message *qtiny.Message, discovery qtiny.Discovery) error {
	message.Type = message.Type | qtiny.MessageTypeBroadcast
	return o.Post(message, discovery)
}

func (o *WebsocketGateway) nodeQueueConsume(event *zk.Event, stat *zk.Stat, data interface{}, box *WatchBox, watcher *ZooWatcher, err error) bool {
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

func (o *WebsocketGateway) messageConsume(conn *zk.Conn, root string, child string) {
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

func (o *WebsocketGateway) GetQueueZNodePath(nodeId string) string {
	return o.pathRootQueue + "/" + nodeId
}

func (o *WebsocketGateway) GetType() string {
	return "zookeeper"
}

func (o *WebsocketGateway) GetMeta() map[string]interface{} {
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
