package qtiny

type PortalKind interface {
	GetType() string
	GetAddress() string
	GetMeta() map[string]interface{}
}

type Portal struct {
	Type       string
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
