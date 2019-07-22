package zookeeper

import (
	"fmt"
	"github.com/camsiabor/go-zookeeper/zk"
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
const PathNodeQueue = "/qnode"
const PathConnection = "/qconn"

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

		_, _ = watcher.Create(PathNano, []byte(""), 0, zk.WorldACL(zk.PermAll))

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

	if remote.RemoteAddresses() == nil {

		var nanoZNodePath = o.GetNanoZNodePath(address)
		var children, _, err = o.watcher.GetConn().Children(nanoZNodePath)
		if err != nil {
			return nil, fmt.Errorf("no consumer found : " + err.Error())
		}
		o.watcher.Watch(WatchTypeChildren, nanoZNodePath, nanoZNodePath, o.nanoRemoteRegistryWatch)
		if children == nil || len(children) == 0 {
			return nil, fmt.Errorf("no consumer found for " + address)
		}
		for i := 0; i < len(children); i++ {
			remote.RemoteAdd(children[i], children[i])
		}
	}
	return remote, nil
}

/* ================================== local ============================================= */

func (o *ZooDiscovery) nanoLocalPublishRegistry(nano *qtiny.Nano) error {
	var parent = o.GetNanoZNodePath(nano.Address)
	var _, err = o.watcher.Create(parent, []byte(""), 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		if o.Logger != nil {
			o.Logger.Println("nano register fail ", parent, " : ", err.Error())
		}
		return err
	}
	var path = o.GetNanoZNodeSelfPath(nano.Address)
	var exist, cerr = o.watcher.Create(path, []byte(""), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if !exist && o.Logger != nil {
		if cerr == nil {
			o.Logger.Println("nano register ", path)
		} else {
			o.Logger.Println("nano register fail ", path, " : ", cerr.Error())
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
		nano.RemoteSet(children, nil)
	}
	return true
}

/* ======================== gateway ========================== */

func (o *ZooDiscovery) hoook(gateway qtiny.Gateway) (*qtiny.Future, error) {
	var future = &qtiny.Future{}

	future.SetRoutine(func(event qtiny.FutureEvent, future *qtiny.Future) qtiny.FutureCallbackReturn {

		var ch = o.watcher.WaitForConnected()
		if ch != nil {
			<-ch
		}

		var pathNodeQueue = fmt.Sprintf("%s/%s", PathNodeQueue, gateway.GetId())
		var pathNodeConnection = fmt.Sprintf("%s/%s", PathConnection, gateway.GetId())

		_, _ = o.watcher.Create(pathNodeQueue, []byte(""), 0, zk.WorldACL(zk.PermAll))

		_, _ = o.watcher.Create(PathConnection, []byte(""), 0, zk.WorldACL(zk.PermAll))
		_, _ = o.watcher.Create(pathNodeConnection, []byte(""), 0, zk.WorldACL(zk.PermAll))
		_, _ = o.watcher.Create(pathNodeConnection+"/"+o.connectId, []byte(""), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))

		future.Succeed(0, nil)

		return 0
	})

	return future, nil
}

/* ======================== path =========================== */

func (o *ZooDiscovery) GetNanoZNodePath(address string) string {
	return PathNano + "/" + address
}

func (o *ZooDiscovery) GetNanoZNodeSelfPath(address string) string {
	return fmt.Sprintf("%s/%s/%s", PathNano, address, o.GetId())
}
