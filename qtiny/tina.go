package qtiny

type Tina struct {
	tinys map[string]Tiny

	gateway     Gateway
	microroller *MicroRoller
}

func (o *Tina) Deploy(tiny Tiny) {

}

func (o *Tina) Undeploy(tiny Tiny) {

}
