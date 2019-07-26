package zookeeper

import (
	"encoding/json"
	"fmt"
	"github.com/camsiabor/go-zookeeper/zk"
	"github.com/camsiabor/qcom/qerr"
	"github.com/camsiabor/qcom/qroutine"
	"github.com/camsiabor/qcom/qstr"
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/impl/memory"
	"github.com/camsiabor/qservice/qtiny"
	"github.com/twinj/uuid"
	"os"
	"time"
)

type ZooDiscovery struct {
	memory.MemDiscovery
	watcher *ZooWatcher

	connectId string

	timer *qroutine.Timer
}

const PathNano = "/qnano"
const PathConnection = "/qportal"

func (o *ZooDiscovery) Init(config map[string]interface{}) error {

	var err error

	if o.watcher == nil {
		o.watcher = &ZooWatcher{}
		o.watcher.Logger = o.Logger
	}

	err = o.watcher.Start(config)

	if err != nil {
		return err
	}

	o.watcher.AddCallback(o.handleConnectionEvents)

	if o.timer != nil {
		o.timer.Stop()
	}

	var scanInterval = util.GetInt(config, 3*60, "scan.interval")

	o.timer = &qroutine.Timer{}
	err = o.timer.Start(0, time.Duration(scanInterval)*time.Second, o.timerloop)
	return err
}

func (o *ZooDiscovery) Start(config map[string]interface{}) error {
	var err error
	defer func() {
		if err != nil {
			_ = o.Stop(config)
		}
	}()
	err = o.MemDiscovery.Start(config)
	if err == nil {
		err = o.Init(config)
	}
	return err
}

func (o *ZooDiscovery) Stop(config map[string]interface{}) error {
	if o.timer != nil {
		o.timer.Stop()
	}
	if o.watcher != nil {
		var err = o.watcher.Stop(config)
		if err != nil {
			return err
		}
	}
	return o.MemDiscovery.Stop(config)
}

func (o *ZooDiscovery) handleConnectionEvents(event *zk.Event, watcher *ZooWatcher, err error) {

	if event.State == zk.StateDisconnected {
		if o.Logger != nil {
			o.Logger.Println("zookeeper discovery disconnected ", o.watcher.Endpoints)
		}
		return
	}

	if event.State == zk.StateConnected || event.State == zk.StateConnectedReadOnly {
		if o.Logger != nil {
			o.Logger.Println("zookeeper discovery connected ", o.watcher.Endpoints)
		}

		if len(o.connectId) == 0 {
			var hostname, _ = os.Hostname()
			o.connectId = hostname + ":" + uuid.NewV4().String()
		}

		_, _ = watcher.Create(PathNano, []byte(""), 0, zk.WorldACL(zk.PermAll), false)
		_, _ = watcher.Create(PathConnection, []byte(""), 0, zk.WorldACL(zk.PermAll), false)

		o.watcher.Watch(WatchTypeChildren, PathConnection, nil, time.Duration(15)*time.Second, o.portalsWatch)

		go func() {
			for i := 0; i < 3; i++ {
				o.nanoLocalPublishRegistries()
				time.Sleep(o.watcher.SessionTimeout + time.Second)
			}
		}()

		o.timer.Wake()

		if o.Logger != nil {
			o.Logger.Println("zookeerp discovery connected setup fin")
		}

	}
}

func (o *ZooDiscovery) timerloop(timer *qroutine.Timer, err error) {
	if err != nil {
		return
	}
	var ch = o.watcher.WaitForConnected()
	if ch != nil {
		<-ch
	}
	o.nanoLocalPublishRegistries()
}

/* ================================== remote ============================================= */

func (o *ZooDiscovery) NanoRemoteGet(address string) (*qtiny.Nano, error) {
	var remote, err = o.MemDiscovery.NanoRemoteGet(address)
	if err != nil {
		return nil, err
	}

	if remote == nil {
		err = o.MemDiscovery.NanoRemoteRegister(&qtiny.Nano{Address: address})
		remote, err = o.MemDiscovery.NanoRemoteGet(address)
		if err != nil {
			return nil, err
		}
	}

	if remote.PortalAddresses() == nil {

		var nanoZNodePath = o.GetNanoZNodePath(address)
		var children, _, err = o.watcher.GetConn().Children(nanoZNodePath)
		if err != nil {
			return nil, fmt.Errorf("no consumer found : " + err.Error())
		}
		o.watcher.Watch(WatchTypeChildren, nanoZNodePath, nil, time.Hour, o.nanoRemoteRegistryWatch)
		if children == nil || len(children) == 0 {
			return nil, fmt.Errorf("no consumer found for " + address)
		}
		for i := 0; i < len(children); i++ {
			remote.PortalAdd(children[i], children[i])
		}
	}
	return remote, nil
}

/* ================================== local ============================================= */

func (o *ZooDiscovery) nanoLocalPublishRegistry(nano *qtiny.Nano) error {
	var parent = o.GetNanoZNodePath(nano.Address)
	var _, err = o.watcher.Create(parent, []byte(""), 0, zk.WorldACL(zk.PermAll), false)
	if err != nil {
		if o.Logger != nil {
			o.Logger.Println("nano register fail ", parent, " : ", err.Error())
		}
		return err
	}

	if o.Gateways == nil {
		return fmt.Errorf("no gateway is set")
	}

	o.GatewaysMutex.RLock()
	o.GatewaysMutex.RUnlock()

	var cerr error
	for _, gateway := range o.Gateways {
		var path = o.GetNanoZNodeSelfPath(nano.Address, gateway)
		var exist, cerr = o.watcher.Create(path, []byte(""), zk.FlagEphemeral, zk.WorldACL(zk.PermAll), false)
		if !exist && o.Logger != nil {
			if cerr == nil {
				o.Logger.Println("nano register ", path)
			} else {
				o.Logger.Println("nano register fail ", path, " : ", cerr.Error())
			}
		}
	}
	return cerr
}

func (o *ZooDiscovery) nanoLocalPublishRegistries() {
	if o.Locals == nil {
		return
	}
	o.LocalsMutex.RLock()
	defer o.LocalsMutex.RUnlock()
	for _, subscriber := range o.Locals {
		go o.nanoLocalPublishRegistry(subscriber)
	}
}

func (o *ZooDiscovery) NanoLocalRegister(nano *qtiny.Nano) error {

	if nano.Flag&qtiny.NanoFlagLocalOnly > 0 {
		return o.MemDiscovery.NanoLocalRegister(nano)
	}

	o.MemDiscovery.NanoLocalAdd(nano)
	if o.watcher.IsConnected() {
		go o.nanoLocalPublishRegistry(nano)
	}
	return nil
}

func (o *ZooDiscovery) nanoRemoteRegistryWatch(event *zk.Event, stat *zk.Stat, data interface{}, box *WatchBox, watcher *ZooWatcher, err error) bool {
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
	var address = qstr.SubLast(box.Path, "/")
	nano, _ := o.MemDiscovery.NanoRemoteGet(address)
	if nano == nil {
		err = o.MemDiscovery.NanoRemoteRegister(&qtiny.Nano{Address: address})
		if err != nil {
			o.Logger.Println(err)
			return true
		}
		nano, _ = o.MemDiscovery.NanoRemoteGet(address)
	}
	if nano != nil {
		nano.PortalSet(children, nil)
	}
	return true
}

/* ======================== gateway ========================== */

func (o *ZooDiscovery) GatewayPublish(gatekey string, gateway qtiny.Gateway) error {
	if err := o.MemDiscovery.GatewayPublish(gatekey, gateway); err != nil {
		return err
	}
	var ch, err = gateway.EventChannelGet(gatekey)
	if err == nil {
		go o.gatewayEventLoop(gateway, ch)
	}
	return err
}

func (o *ZooDiscovery) gatewayEventLoop(gateway qtiny.Gateway, ch <-chan *qtiny.GatewayEventBox) {

	var pathNodeConnection string

	defer func() {
		_ = o.watcher.Delete(pathNodeConnection, true, true)
	}()

	var connected = false
	for box := range ch {
		func() {
			defer func() {
				var pan = recover()
				if pan != nil && o.Logger != nil {
					o.Logger.Printf("gateway publish loop error %v \n %v", util.AsError(pan).Error(), qerr.StackString("", 1, 1024))
				}
			}()

			pathNodeConnection = fmt.Sprintf("%s/%s.%s", PathConnection, gateway.GetNodeId(), gateway.GetId())

			if box.Event == qtiny.GatewayEventDisconnected {
				connected = false
				_ = o.watcher.Delete(pathNodeConnection, true, true)
				return
			}

			if box.Event != qtiny.GatewayEventConnected {
				return
			}

			connected = true
			go o.gatewayPublish(&connected, box, pathNodeConnection, gateway)

		}()
	}
}

func (o *ZooDiscovery) gatewayPublish(connected *bool, box *qtiny.GatewayEventBox, pathNodeConnection string, gateway qtiny.Gateway) {

	var sleep = false
	for *connected {
		if sleep {
			time.Sleep(time.Second * 3)
		}
		var data, _ = json.Marshal(box.Meta)
		_, _ = o.watcher.Create(PathConnection, []byte(""), 0, zk.WorldACL(zk.PermAll), false)
		_, err := o.watcher.Create(pathNodeConnection, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll), true)
		if err != nil {
			sleep = true
			continue
		}
		_, stat, err := o.watcher.GetConn().Get(pathNodeConnection)
		if err != nil || stat == nil {
			sleep = true
			continue
		}
		if stat.EphemeralOwner != o.watcher.GetConn().SessionID() {
			sleep = true
			continue
		}
		o.Logger.Printf("zookeeper gateway publish %v - %v", gateway.GetType(), gateway.GetId())
		return
	}

}

/* ======================== portal ========================= */

func (o *ZooDiscovery) portalsWatch(event *zk.Event, stat *zk.Stat, data interface{}, box *WatchBox, watcher *ZooWatcher, err error) bool {

	if err != nil && o.Logger != nil {
		o.Logger.Println("portal registry watch error", err.Error())
		return true
	}
	if data == nil {
		return true
	}
	var children, _ = data.([]string)

	if len(children) == 0 {
		return true
	}

	var addresses = make(map[string]int)
	for _, address := range children {
		addresses[address] = 1
	}

	var removes = make(map[string]*qtiny.Portal)

	func() {

		o.PortalsMutex.Lock()
		defer o.PortalsMutex.Unlock()

		if o.Portals == nil {
			return
		}
		for address, portal := range o.Portals {
			if addresses[address] == 0 {
				removes[address] = portal
			}
		}
	}()

	if len(removes) > 0 {

		o.Logger.Printf("portal registry %v removes %v -> [%v]", box.Path, removes, children)

		for address, portal := range removes {
			o.watcher.UnWatch(WatchTypeGet, portal.Path)
			o.PortalRemove(address)
			o.Logger.Printf("portal %v remove", portal.Address)
		}

	}

	var news []string
	for _, address := range children {
		var current = o.MemDiscovery.PortalGet(address)
		if current == nil {
			if news == nil {
				news = []string{address}
			} else {
				news = append(news, address)
			}
		}
	}

	if news == nil {
		return true
	}

	o.Logger.Printf("portal registry %v creates %v -> [%v]", box.Path, news, children)

	for _, address := range news {
		var portal = o.MemDiscovery.PortalCreate(address)
		portal.Path = box.Path + "/" + address
		o.watcher.Watch(WatchTypeGet, portal.Path, portal, time.Duration(15)*time.Minute, o.portalWatch)
	}

	return true
}

func (o *ZooDiscovery) portalWatch(event *zk.Event, stat *zk.Stat, data interface{}, box *WatchBox, watcher *ZooWatcher, err error) bool {
	if err != nil {
		o.Logger.Printf("%v watch error %v", box.Path, err.Error())
		return true
	}
	var portal = box.Data.(*qtiny.Portal)
	if data == nil {
		return true
	}
	var bytes = data.([]byte)
	var meta map[string]interface{}
	err = json.Unmarshal(bytes, &meta)
	if err != nil {
		o.Logger.Printf("%v watch parse error %v", box.Path, err.Error())
		return true
	}

	if meta != nil {
		portal.Meta = meta
	}

	o.Logger.Printf("portal %v meta change", portal.Address)

	portal.Type = util.GetStr(portal.Meta, "", "type")

	return true
}

/* ======================== path =========================== */

func (o *ZooDiscovery) GetNanoZNodePath(address string) string {
	return PathNano + "/" + address
}

func (o *ZooDiscovery) GetNanoZNodeSelfPath(address string, gateway qtiny.Gateway) string {
	return fmt.Sprintf("%s/%s/%s.%s", PathNano, address, gateway.GetNodeId(), gateway.GetId())
}
