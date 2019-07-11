package zookeeper

import (
	"sync"
)

var zkwMutex sync.Mutex
var zkwInstances = map[string]*ZooWatcher{}

func ZooWatcherGet(id string, endpoint string) (instance *ZooWatcher, err error) {

	zkwMutex.Lock()
	instance = zkwInstances[id]
	zkwMutex.Unlock()

	if instance == nil {
		zkwMutex.Lock()
		defer zkwMutex.Unlock()
		instance = zkwInstances[id]
		if instance == nil {
			instance = &ZooWatcher{}
			instance.Id = id
			zkwInstances[id] = instance
		}
	}

	if len(endpoint) > 0 && !instance.IsConnected() {
		instance.Endpoints = []string{endpoint}
		err = instance.Start(nil)
	}
	return instance, err
}
