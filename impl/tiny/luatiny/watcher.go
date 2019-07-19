package luatiny

import (
	"fmt"
	"github.com/fsnotify/fsnotify"
)

func (o *LuaTinyGuide) watcherStart() {

	var err error
	o.watcherConfig, err = fsnotify.NewWatcher()
	if err != nil {
		panic(err)
	}
	o.watcherScript, err = fsnotify.NewWatcher()
	if err != nil {
		panic(err)
	}

	o.watcherConfig.Add(o.ConfigPath)

	// TODO iterate
	func() {
		o.unitMutex.RLock()
		defer o.unitMutex.RUnlock()
		for _, v := range o.units {
			if v.L == nil {
				continue
			}
			o.watcherScript.Add(v.path)
		}
	}()

	go o.watchingConfig()
	go o.watchingScript()
}

func (o *LuaTinyGuide) watcherStop() {
	if o.watcherConfig != nil {
		o.watcherConfig.Close()
	}
	if o.watcherScript != nil {
		o.watcherScript.Close()
	}
}

func (o *LuaTinyGuide) watchingConfig() {
	defer func() {
		var pan = recover()
		if pan != nil {
			o.Logger.Println(pan)
		}
	}()
	for {
		select {
		case event, ok := <-o.watcherConfig.Events:
			if !ok {
				return
			}
			o.Logger.Println(event.String())
			o.watcherConfig.Remove(o.ConfigPath)
			o.watcherConfig.Add(o.ConfigPath)
		case err, ok := <-o.watcherConfig.Errors:
			if !ok {
				return
			}
			o.Logger.Println(err)
		}
	}
	o.Logger.Println("out!")
}

func (o *LuaTinyGuide) watchingScript() {
	for {
		select {
		case event, ok := <-o.watcherConfig.Events:
			if !ok {
				return
			}
			if event.Op&fsnotify.Write == fsnotify.Write {

			}

		case err, ok := <-o.watcherConfig.Errors:
			if !ok {
				return
			}
			fmt.Println(err)
		}
	}
}
