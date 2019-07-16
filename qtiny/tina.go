package qtiny

import (
	"github.com/camsiabor/qcom/util"
	"github.com/twinj/uuid"
	"log"
	"os"
	"sync"
)

var tina = &Tina{}

func GetTina() *Tina {
	return tina
}

type Tina struct {
	tinaMutex sync.RWMutex

	tinyMutex sync.RWMutex
	tinys     map[string]*Tiny

	gateway     Gateway
	microroller *Microroller

	config map[string]interface{}

	logger *log.Logger
}

func (o *Tina) Start(config map[string]interface{}) error {

	o.tinaMutex.Lock()
	defer o.tinaMutex.Unlock()

	if o.tinys == nil {
		o.tinys = make(map[string]*Tiny)
	}

	o.config = config
	if o.config == nil {
		o.config = make(map[string]interface{})
	}
	var loggerConfig = util.GetMap(o.config, true, "logger")
	var err = o.initLogger(loggerConfig)
	if err != nil {
		return err
	}
	var gatewayConfig = util.GetMap(o.config, true, "gateway")
	err = o.initGateway(gatewayConfig)
	return err
}

func (o *Tina) Stop() error {

	o.tinaMutex.Lock()
	defer o.tinaMutex.Unlock()

	return nil
}

func (o *Tina) initLogger(map[string]interface{}) error {
	if o.logger != nil {
		return nil
	}
	// TODO
	o.logger = log.New(os.Stdout, "tina", log.Lshortfile|log.LstdFlags|log.LUTC)
	return nil
}

func (o *Tina) initGateway(config map[string]interface{}) error {

	if o.gateway == nil {
		panic("gateway is not set yet")
	}

	if o.gateway.GetLogger() == nil {
		o.gateway.SetLogger(o.logger)
	}
	if err := o.gateway.Start(config); err != nil {
		return err
	}
	if o.microroller == nil {
		o.microroller = &Microroller{}
	}
	if o.microroller.GetLogger() == nil {
		o.microroller.SetLogger(o.logger)
	}
	o.microroller.SetGateway(o.gateway)
	return o.microroller.Start(config)
}

func (o *Tina) Deploy(id string, guide *TinyGuide, config map[string]interface{}, flag TinyFlag, options TinyOptions) (Future, error) {

	var future = &FutureImpl{}

	if guide.Start == nil {
		_ = future.Fail(0, "no start routine is set in tiny guide")
		return future, util.AsError(future.errCause)
	}

	o.tinyMutex.RLock()
	var current = o.tinys[id]
	o.tinyMutex.RUnlock()

	if current != nil {
		_ = future.Fail(0, "already deployed a tiny with id : "+id)
		return nil, util.AsError(future.errCause)
	}

	if len(id) == 0 {
		id = uuid.NewV4().String()
	}

	var tiny = &Tiny{}
	tiny.id = uuid.NewV4().String()
	tiny.flag = flag
	tiny.config = config
	tiny.options = options
	tiny.guide = guide
	tiny.tina = o

	o.tinyMutex.Lock()
	o.tinys[tiny.id] = tiny
	o.tinyMutex.Unlock()

	if flag&TinyFlagDeploySync > 0 {
		tiny.guide.Start(tiny, future)
	} else {
		go tiny.guide.Start(tiny, future)
	}

	return future, nil
}

func (o *Tina) Undeploy(id string) (Future, error) {
	return nil, nil
}

func (o *Tina) SetGateway(gateway Gateway) *Tina {
	o.gateway = gateway
	return o
}

func (o *Tina) GetGateway() Gateway {
	return o.gateway
}

func (o *Tina) SetMicroroller(microroller *Microroller) *Tina {
	o.microroller = microroller
	return o
}

func (o *Tina) GetMicroroller() *Microroller {
	return o.microroller
}

func (o *Tina) GetLogger() *log.Logger {
	return o.logger
}

func (o *Tina) SetLogger(logger *log.Logger) *Tina {
	o.logger = logger
	return o
}
