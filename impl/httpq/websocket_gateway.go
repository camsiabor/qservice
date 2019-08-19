package httpq

import (
	"fmt"
	"github.com/camsiabor/qcom/qerr"
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
	mutex    sync.RWMutex
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
			o.Logger.Printf("[websocket] gateway listening to %v", o.Port)
			func() {
				time.Sleep(time.Second)
				if o.wsserver != nil {
					o.EventChannelSend(qtiny.GatewayEventConnected, o.GetMeta())
				}
			}()
			err = o.wsserver.ListenAndServe()
			if err != nil {
				o.EventChannelSend(qtiny.GatewayEventDisconnected, o.GetMeta())
				o.wsserver = nil
				o.Logger.Printf("[websocket] gateway listen & serve %v error %v ", o.Port, err.Error())
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
		o.Logger.Printf("[websocket] gateway close")
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

	// TODO authentication

	var conn, err = o.wsupgrader.Upgrade(response, request, nil)
	if err != nil {
		err = qerr.StackErr(0, 1024, err, "update request to ws fail %v", request.RemoteAddr)
		if err == nil {
			return
		}
		_, _ = response.Write([]byte(err.Error()))
		o.Logger.Printf(err.Error())
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

	go o.recvLoop(session)
}

func (o *WebsocketGateway) recvLoop(client *wssession) {

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
			log.Printf("[websocket] %v read message error %v", client.id, err.Error())
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
			err = message.FromJson(data)
			if err != nil {
				o.Logger.Printf("[websocket] %v parse message codec error %v", client.id, err.Error())
				return
			}

			if o.Verbose > 0 {
				o.Logger.Printf("[websocket] %v recv | %v", client.id, message.String())
			}

			message.Session = client.id
			message.SessionRelated = messageType
			o.Queue <- message
		}()
	}
}

func (o *WebsocketGateway) portalSessionGet(portalAddress string) *wssession {
	o.wsportalsMutex.RLock()
	var wsportal = o.wsportals[portalAddress]
	o.wsportalsMutex.RUnlock()

	if wsportal != nil {
		return wsportal
	}

	func() {
		o.wsportalsMutex.Lock()
		defer o.wsportalsMutex.Unlock()
		wsportal = o.wsportals[portalAddress]
		if wsportal == nil {
			wsportal = &wssession{}
			wsportal.id = portalAddress
			wsportal.timeConn = time.Now()
			o.wsportals[portalAddress] = wsportal
		}
	}()

	return wsportal
}

func (o *WebsocketGateway) portalSessionConnect(wsportal *wssession, portal qtiny.PortalKind, portalAddress string, traceDepth int, force bool) error {
	if !force && wsportal.conn != nil {
		return nil
	}
	var meta = portal.GetMeta()
	if meta == nil || len(meta) == 0 {
		return qerr.StackStringErr(0, traceDepth, "portal %v is not ready, no meta is set ", portalAddress)
	}

	var endpoints = util.GetStringSlice(meta, "endpoints")
	if endpoints == nil || len(endpoints) == 0 {
		return qerr.StackStringErr(0, traceDepth, "portal %v endpoints is not set", portalAddress)
	}

	var err error
	wsportal.mutex.Lock()
	defer wsportal.mutex.Unlock()

	for _, endpoint := range endpoints {

		o.Logger.Printf("[websocket] connecting to portal %v | %v", portalAddress, endpoint)
		wsportal.conn, _, err = websocket.DefaultDialer.Dial(endpoint, nil)
		if err != nil && wsportal.conn != nil {
			wsportal.conn.Close()
			wsportal.conn = nil
		}
		if err == nil && wsportal.conn != nil {
			o.Logger.Printf("[websocket] connected to portal %v | %v", portalAddress, endpoint)
			return nil
		}
	}

	return qerr.StackErr(0, traceDepth, err, "")
}

func (o *WebsocketGateway) publish(
	messageType qtiny.MessageType,
	portalAddress string, portal qtiny.PortalKind,
	remote *qtiny.Nano, message *qtiny.Message,
	discovery qtiny.Discovery, gateway qtiny.Gateway, data []byte) (err error) {

	defer func() {
		var pan = recover()
		if pan != nil {
			o.Logger.Println(qerr.StackString(1, 1024, ""))
		}
	}()

	if o.Verbose > 0 {
		o.Logger.Printf(qerr.StackString(0, o.Verbose, "[websocket] publishing %v", message.String()))
	}

	var wsportal = o.portalSessionGet(portalAddress)

	var traceDepth = message.GetTraceDepth()
	if wsportal.conn == nil {
		if portal == nil || len(portal.GetType()) == 0 {
			err = fmt.Errorf("invalid portal %v", portalAddress)
			if o.Verbose > 0 {
				o.Logger.Printf(qerr.StackString(0, o.Verbose, "[websocket] %v", err.Error()))
			}
			return err
		}
		if err = o.portalSessionConnect(wsportal, portal, portalAddress, traceDepth, true); err != nil {
			if o.Verbose > 0 {
				o.Logger.Printf(qerr.StackString(0, o.Verbose, "[websocket] portal disconnected %v | %v", portalAddress, err.Error()))
			}
			return err
		}
	}
	wsportal.mutex.Lock()
	err = wsportal.conn.WriteMessage(websocket.BinaryMessage, data)
	wsportal.mutex.Unlock()
	if err != nil {
		err = o.portalSessionConnect(wsportal, portal, portalAddress, traceDepth, true)
	}
	if err != nil {
		if o.Verbose > 0 {
			o.Logger.Printf(qerr.StackString(0, o.Verbose, "[websocket] portal disconnected %v | %v", portalAddress, err.Error()))
		}
	}
	return err
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
	if len(o.Type) == 0 {
		o.Type = "[websocket]"
	}
	return o.Type
}

func (o *WebsocketGateway) GetMeta() map[string]interface{} {
	if o.Meta == nil {
		var hostname, _ = os.Hostname()
		o.Meta = make(map[string]interface{})
		o.Meta["id"] = o.GetId()
		o.Meta["type"] = o.GetType()
		o.Meta["hostname"] = hostname
	}

	o.Meta["endpoints"] = o.Endpoints

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
