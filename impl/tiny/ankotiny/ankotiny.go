package ankotiny

import (
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/qtiny"
	"github.com/mattn/anko/packages"
	"github.com/mattn/anko/vm"
	"io/ioutil"
	"log"
	"path/filepath"
	"sync"
)

type AnkoTinyGuide struct {
	qtiny.TinyGuide

	Path string
	Main string

	mutex sync.RWMutex

	env *vm.Env

	config map[string]interface{}

	tiny qtiny.TinyKind
}

func NewAnkoTinyGuide(base string, main string) *AnkoTinyGuide {
	var guide = &AnkoTinyGuide{}
	if len(main) == 0 {
		panic("anko tiny guide main is not set")
	}

	var err error
	guide.Path, err = filepath.Abs(base)
	if err != nil {
		panic(err)
	}

	guide.Main = main
	guide.TinyGuide.Start = guide.start
	guide.TinyGuide.Stop = guide.stop
	return guide
}

func (o *AnkoTinyGuide) start(event qtiny.TinyGuideEvent, tiny qtiny.TinyKind, guide qtiny.TinyGuideKind, config map[string]interface{}, future *qtiny.Future, err error) {

	if err != nil {
		log.Printf("zookeeper tiny guide start error %v", err)
		return
	}

	defer func() {
		if err != nil {
			future.Fail(0, err)
		}
	}()

	o.mutex.Lock()
	defer o.mutex.Unlock()

	o.tiny = tiny

	if len(o.Main) == 0 {
		o.Main = util.GetStr(config, "", "main")
	}

	if len(o.Main) == 0 {
		future.Fail(0, "main is not set")
		return
	}

	o.env = vm.NewEnv()
	packages.DefineImport(o.env)

	err = o.define(tiny)

	bytes, err := ioutil.ReadFile(o.Path + "/" + o.Main)

	if err != nil {
		return
	}

	content := string(bytes)

	_, err = o.env.Execute(content)
	if err != nil {
		return
	}

	future.Succeed(0, nil)

}

func (o *AnkoTinyGuide) define(tiny qtiny.TinyKind) error {
	var err = o.env.Define("tiny", tiny)
	if err != nil {
		return err
	}
	return nil

}

func (o *AnkoTinyGuide) stop(event qtiny.TinyGuideEvent, tiny qtiny.TinyKind, guide qtiny.TinyGuideKind, config map[string]interface{}, future *qtiny.Future, err error) {

	if err != nil {
		log.Printf("zookeeper tiny guide stop error %v", err)
		return
	}

	o.mutex.Lock()
	defer o.mutex.Unlock()

}
