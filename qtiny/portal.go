package qtiny

import (
	"github.com/camsiabor/qcom/util"
	"hash/fnv"
)

type PortalKind interface {
	GetType() string
	GetTypeHash() uint32
	GetAddress() string
	GetMeta() map[string]interface{}
}

type Portal struct {
	Type       string
	TypeHash   uint32
	Path       string
	Address    string
	Discoverer Discovery
	Meta       map[string]interface{}
}

func (o *Portal) GetType() string {
	return o.Type
}

func (o *Portal) GetAddress() string {
	return o.Address
}

func (o *Portal) GetMeta() map[string]interface{} {
	return o.Meta
}

func (o *Portal) SetMeta(meta map[string]interface{}) {
	o.Meta = meta
	if meta == nil {
		return
	}
	o.Type = util.GetStr(o.Meta, "", "type")
}

func (o *Portal) GetTypeHash() uint32 {
	if o.TypeHash == 0 && len(o.Type) > 0 {
		var hash = fnv.New32a()
		var _, err = hash.Write([]byte(o.Type))
		if err != nil {
			panic(err)
		}
		o.TypeHash = hash.Sum32()
	}
	return o.TypeHash
}
