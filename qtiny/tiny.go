package qtiny

type TinyOptions map[string]interface{}
type TinyFlag int

type Tiny struct {
	id    string
	group string
	tina  *Tina
}

func (o *Tiny) GetTina() *Tina {
	return o.tina
}

func (o *Tiny) GetId() string {
	return o.id
}

func (o *Tiny) GetGroup() string {
	return o.group
}
