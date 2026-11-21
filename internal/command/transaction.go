package command

import (
	"errors"

	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

type serverManagedCommand struct {
	name string
}

func (c *serverManagedCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	return protocol.MakeError(errors.New("ERR " + c.name + " is managed by the server connection state"))
}

type MultiCommand struct{ serverManagedCommand }
type ExecCommand struct{ serverManagedCommand }
type DiscardCommand struct{ serverManagedCommand }
type WatchCommand struct{ serverManagedCommand }
type UnwatchCommand struct{ serverManagedCommand }
type SubscribeCommand struct{ serverManagedCommand }
type PSubscribeCommand struct{ serverManagedCommand }
type UnsubscribeCommand struct{ serverManagedCommand }
type PUnsubscribeCommand struct{ serverManagedCommand }
type PublishCommand struct{ serverManagedCommand }
type PubSubCommand struct{ serverManagedCommand }
type CopyCommand struct{ serverManagedCommand }
type MoveCommand struct{ serverManagedCommand }
