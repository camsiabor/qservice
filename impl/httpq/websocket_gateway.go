package httpq

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

	Port int

	Endpoints []string
	IPS       []string

	wsserver *http.Server

	wssessionsMutex sync.RWMutex
	wssessions      map[string]*wssession

	wsportalsMutex sync.RWMutex
	wsportals      map[string]*wssession

	wsupgrader *websocket.Upgrader
}

type wssession struct {
	id       string
	conn     *websocket.Conn
	request  *http.Request
	timeConn time.Time
}

func (o *WebsocketGateway) Init(config map[string]interface{}) error {

	var err error

	o.GetMeta()

	o.Port = util.GetInt(config, 8080, "port")
	o.Endpoints = util.GetStringSlice(config, "endpoints")

	if o.wsupgrader == nil {
		o.wsupgrader = &websocket.Upgrader{}
	}

	if o.wsserver == nil {
		o.wsserver = &http.Server{
			Addr:    fmt.Sprintf(":%v", o.Port),
			Handler: o,
		}

		// TODO connect event
		go func() {
			o.Logger.Printf("websocket gateway listening to %v", o.Port)
			err = o.wsserver.ListenAndServe()
			if err != nil {
				o.wsserver = nil
				o.Logger.Printf("websocket gateway listen & serve %v error %v ", o.Port, err.Error())
			}
		}()

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

	o.wsportals = make(map[string]*wssession)
	o.wssessions = make(map[string]*wssession)

	err = o.MemGateway.Start(config)
	if err == nil {
		err = o.Init(config)
	}
	return err
}

func (o *WebsocketGateway) Stop(config map[string]interface{}) error {

	o.Mutex.Lock()
	defer o.Mutex.Unlock()

	if o.wsserver != nil {
		_ = o.wsserver.Close()
		o.wsserver = nil
		o.Logger.Printf("websocket gateway close")
	}

	if o.wsportals != nil {
		for _, wsportal := range o.wsportals {
			wsportal.conn.Close()
		}
		o.wsportals = nil
	}

	if o.wssessions != nil {
		for _, session := range o.wssessions {
			session.conn.Close()
		}
		o.wssessions = nil
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

	var session = &wssession{
		conn:     conn,
		request:  request,
		timeConn: time.Now(),
	}
	session.id = fmt.Sprintf("%v:%v", request.RemoteAddr, time.Now().Nanosecond())

	func() {
		o.wssessionsMutex.Lock()
		defer o.wssessionsMutex.Unlock()
		o.wssessions[session.id] = session
	}()

	go o.handleRead(session)
}

func (o *WebsocketGateway) handleRead(client *wssession) {
	defer func() {
		o.wssessionsMutex.Lock()
		defer o.wssessionsMutex.Unlock()
		delete(o.wssessions, client.id)
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

	o.wsportalsMutex.RLock()
	var wsportal = o.wsportals[portalAddress]
	o.wsportalsMutex.RUnlock()

	var err error
	wsportal.conn, _, err = websocket.DefaultDialer.Dial("", nil)
	if err != nil {

	}

	if portal == nil || len(portal.GetType()) == 0 {
		return fmt.Errorf("invalid portal %v", portalAddress)
	}

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

	if o.Meta["port"] == nil && o.Port > 0 {
		o.Meta["port"] = o.Port
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
