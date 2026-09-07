//go:build windows

package windowspty_test

import (
	"context"

	"github.com/aruzen/streammux/pty"
	"github.com/aruzen/streammux/pty/windowspty"
)

func ExampleManagedFactory() {
	manager, err := pty.NewManager(
		context.Background(),
		windowspty.ManagedFactory{},
		pty.DefaultManagerConfig(),
	)
	if err != nil {
		return
	}
	defer manager.Close()
}
