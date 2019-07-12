package zookeeper

import (
	"fmt"
	"github.com/camsiabor/go-zookeeper/zk"
	"github.com/camsiabor/qservice/core"
	"github.com/camsiabor/qservice/impl/memory"
)

type ZGateway struct {
	memory.MGateway

	watcher *ZooWatcher
}

const PathService = "/service"

func (o *ZGateway) Init(config map[string]interface{}) error {

	var err error
	if o.watcher == nil {
		o.watcher = &ZooWatcher{}
	}

	err = o.watcher.Start(config)

	if err != nil {
		return err
	}

	o.watcher.AddConnectCallback(func(event *zk.Event, watcher *ZooWatcher, err error) {
		if event.State == zk.StateConnected || event.State == zk.StateConnectedReadOnly {
			var conn = o.watcher.GetConn()
			var exist, _, _ = conn.Exists(PathService)
			if !exist {
				_, _ = conn.Create(PathService, []byte(""), 0, zk.WorldACL(zk.PermAll))
			}
		}
	})

	return nil
}

func (o *ZGateway) Start(config map[string]interface{}) error {
	var err error
	defer func() {
		if err != nil {
			_ = o.Stop(config)
		}
	}()
	err = o.Init(config)
	if err == nil {
		err = o.MGateway.Start(config)
	}
	return err
}

func (o *ZGateway) Stop(config map[string]interface{}) error {
	if o.watcher != nil {
		var err = o.watcher.Stop(config)
		if err != nil {
			return err
		}
	}
	return o.MGateway.Stop(config)
}

func (o *ZGateway) Loop() {
	var ok bool
	var msg *core.Message
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

func (o *ZGateway) Poll(limit int) (chan *core.Message, error) {

	if limit <= 0 {
		limit = 8192
	}

	var ch = make(chan *core.Message, limit)

	o.Mutex.Lock()
	defer o.Mutex.Unlock()

	if o.Listeners == nil {
		o.Listeners = make([]chan *core.Message, 1)
		o.Listeners[0] = ch
	} else {
		o.Listeners = append(o.Listeners, ch)
	}

	return ch, nil
}

func (o *ZGateway) Post(message *core.Message) error {
	if o.Queue == nil {
		return fmt.Errorf("gateway not started yet")
	}
	o.Queue <- message
	return nil
}

func (o *ZGateway) Broadcast(message *core.Message) error {
	if o.Queue == nil {
		return fmt.Errorf("gateway not started yet")
	}
	o.Queue <- message
	return nil
}

func (o ZGateway) ServiceRegister(address string, options core.ServiceOptions) error {
	// TODO
	/*
		var path, err = o.conn.Create("/service/" + address + "/", []byte(""), zk.FlagEphemeral | zk.FlagSequence, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
		fmt.Println(path)
	*/

	return nil
}

func (o ZGateway) ServiceUnregister(address string) error {
	// TODO
	//return o.conn.Delete("/service/"+address+"/", 0)
	return nil
}
