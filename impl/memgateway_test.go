package impl

import (
	"fmt"
	"github.com/camsiabor/qservice/core"
	"os"
	"testing"
)

func TestMemoryGateway(t *testing.T) {

	var gateway = &MemoryGateway{}
	var overseer = &core.Overseer{}

	overseer.Init(gateway)

	if err := gateway.Start(); err != nil {
		panic(err)
	}

	if err := overseer.Start(); err != nil {
		panic(err)
	}

	err := overseer.ServiceRegister("qam.helloworld", nil, func(message *core.Message) {
		_, _ = fmt.Fprintf(os.Stdout, "receive %s %v", message.Address, message.Data)
	})

	if err != nil {
		panic(err)
	}

}
