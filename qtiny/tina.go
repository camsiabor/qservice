package qtiny

import (
	"github.com/camsiabor/qcom/util"
	"github.com/twinj/uuid"
	"log"
	"os"
)

var tina = &Tina{}

func GetTina() *Tina {
	return tina
}

type Tina struct {
	tinys map[string]Tiny

	gateway     Gateway
	microroller *Microroller

	config map[string]interface{}

	logger *log.Logger
}

func (o *Tina) Start(config map[string]interface{}) error {
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

	// default zookeeper gateway presently
	o.gateway.SetLogger(o.logger)
	if err := o.gateway.Start(config); err != nil {
		return err
	}
	o.microroller.SetGateway(o.gateway)
	return o.microroller.Start(config)
}

func (o *Tina) Deploy(guide TinyGuide, config map[string]interface{}, flag TinyFlag, options TinyOptions) error {

	var tiny = &Tiny{}
	tiny.id = uuid.NewV4().String()
	tiny.group = uuid.NewV4().String()

	go guide.Start(tiny, nil)

	return nil
}

func (o *Tina) Undeploy(deployId string) error {
	return nil
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
