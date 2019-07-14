package qtiny

type Nano interface {
	GetTiny() *Tiny
}

type NanoImpl struct {
	tiny *Tiny
}
