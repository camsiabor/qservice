package zookeeper

import (
	"fmt"
	"github.com/camsiabor/qservice/core"
	"github.com/camsiabor/qservice/impl/memory"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

type ZGateway struct {
	memory.MGateway

	Endpoints      []string
	SessionTimeout time.Duration

	conn         *zk.Conn
	eventChannel <-chan zk.Event
}

func (o *ZGateway) Start(args ...interface{}) error {

	var err error
	defer func() {
		if err != nil {
			_ = o.Stop()
		}
	}()

	err = o.ZkInit()
	if err == nil {
		err = o.MGateway.Start(args)
	}
	return err
}

func (o *ZGateway) ZkInit() error {
	if o.Endpoints == nil {
		o.Endpoints = []string{"127.0.0.1:2181"}
	}
	if o.SessionTimeout <= 0 {
		o.SessionTimeout = time.Duration(10) * time.Second
	}

	var err error
	o.conn, o.eventChannel, err = zk.Connect(o.Endpoints, o.SessionTimeout)
	if err != nil {
		return err
	}

	var exist bool
	exist, _, err = o.conn.Exists("/service")
	if err != nil {
		return err
	}
	if !exist {
		_, err = o.conn.Create("/service", []byte(""), 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}

	return nil
}

func (o *ZGateway) Stop(args ...interface{}) error {
	if o.conn != nil {
		o.conn.Close()
	}
	return o.MGateway.Stop(args...)
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
	return o.conn.Delete("/service/"+address+"/", 0)
}
