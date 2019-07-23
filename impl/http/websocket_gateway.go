package http

import (
	"fmt"
	"github.com/camsiabor/qcom/qnet"
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/impl/memory"
	"github.com/camsiabor/qservice/qtiny"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

type WebsocketGateway struct {
	memory.MemGateway

	Bind string

	Endpoints []string
	IPS       []string

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

	o.Bind = util.GetStr(config, ":8080", "bind")
	o.Endpoints = util.GetStringSlice(config, "endpoints")

	if o.wsupgrader == nil {
		o.wsupgrader = &websocket.Upgrader{}
	}

	if o.wsserver == nil {
		o.wsserver = &http.Server{
			Addr:    o.Bind,
			Handler: o,
		}
		err = o.wsserver.ListenAndServe()
		if err == nil {
			o.Logger.Printf("websocket gateway listening to %v", o.Bind)
		} else {
			o.Logger.Printf("websocket gateway listen & serve %v error %v ", o.Bind, err.Error())
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

	if o.Publisher == nil {
		o.Publisher = o.publish
	}

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

	go o.handleRead(client)
}

func (o *WebsocketGateway) handleRead(client *wsclient) {
	defer func() {
		o.wsclientsMutex.Lock()
		defer o.wsclientsMutex.Unlock()
		delete(o.wsclients, client.id)
	}()
	defer func() {
		client.conn.Close()
	}()

	for {
		messageType, data, err := client.conn.ReadMessage()
		if err != nil {
			log.Printf("websocket %v read message error %v", client.id, err.Error())
			break
		}
		func() {
			defer func() {
				var pan = recover()
				if pan != nil {
					o.Logger.Printf("parse %v message codec fail %v", client.id, util.AsError(pan).Error())
				}
			}()
			var message = &qtiny.Message{}
			message.FromJson(data)
			message.Session = client.id
			message.SessionRelated = messageType
			o.Queue <- message
		}()
	}

}

func (o *WebsocketGateway) publish(
	messageType qtiny.MessageType,
	portalAddress string, portal qtiny.PortalKind,
	remote *qtiny.Nano, message *qtiny.Message,
	discovery qtiny.Discovery, gateway qtiny.Gateway, data []byte) error {

	// TODO

	return nil
}
func (o *WebsocketGateway) Post(message *qtiny.Message, discovery qtiny.Discovery) error {
	return o.MemGateway.Publish(message, discovery)
}

func (o *WebsocketGateway) Multicast(message *qtiny.Message, discovery qtiny.Discovery) error {
	message.Type = message.Type | qtiny.MessageTypeMulticast
	return o.Post(message, discovery)
}

func (o *WebsocketGateway) Broadcast(message *qtiny.Message, discovery qtiny.Discovery) error {
	message.Type = message.Type | qtiny.MessageTypeBroadcast
	return o.Post(message, discovery)
}

func (o *WebsocketGateway) GetType() string {
	return "websocket"
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
		o.Meta["endpoints"] = o.Endpoints
	}

	if o.Meta["ips"] == nil {
		var ips, err = qnet.AllNetInterfaceIPString()
		if err != nil && o.Logger != nil {
			o.Logger.Printf("get all net interface ip fail %v", err.Error())
		}
		o.Meta["ips"] = ips
	}

	return o.Meta
}
