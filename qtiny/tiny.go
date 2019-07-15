package qtiny

type TinyOptions map[string]interface{}

type Tiny interface {
	LifeCycler

	Next() Tiny
	Prev() Tiny

	GetGroup() string
	SetGroup(string) Tiny

	GetGateway() Gateway
	GetOverseer() *Overseer
}
