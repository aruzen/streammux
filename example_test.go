package streammux_test

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/aruzen/streammux"
	"github.com/aruzen/streammux/pty"
)

type exampleFactory struct{}

func (exampleFactory) StartManaged(context.Context, pty.ProcessSpec) (pty.ManagedProcess, error) {
	return nil, errors.New("example backend")
}

func ExamplePeer_protocol() {
	ctx, cancel := context.WithCancel(context.Background())
	left, right := net.Pipe()
	defer right.Close()

	conn, _ := streammux.Open(ctx, left, streammux.DefaultConfig())
	peer, _ := streammux.NewPeer(conn, streammux.DefaultPeerConfig())
	managerConfig := pty.DefaultManagerConfig()
	managerConfig.SessionLifecycle = pty.SessionLifecycleDescriptor{
		ExitPolicy:          pty.ExitedSessionRetain,
		MaxRetainedSessions: 64,
	}
	manager, _ := pty.NewManager(ctx, exampleFactory{}, managerConfig)
	protocol, _ := pty.RegisterProtocol(peer, manager, pty.ProtocolConfig{
		Version: 1,
		Types: pty.MessageTypes{
			Open: 10, List: 11, Inspect: 12, Attach: 13, Detach: 14,
			Input: 15, Output: 16, Resize: 17, Kill: 18, Exit: 19,
			ReplayBegin: 20, ReplayEnd: 21, Error: 22,
		},
	})

	fmt.Println("ready")
	_ = protocol.Close()
	_ = peer.Close()
	_ = manager.Close()
	cancel()
	// Output: ready
}
