package server

import (
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/database"
)

func TestPingCanRunAlongsideOrdinaryWrite(t *testing.T) {
	manager := newExecutionTestManager(t)
	db := manager.GetDefaultDB()
	plain := NewConnectionContext(db, nil, nil)
	hub := NewPubSubHub()
	subscribed := NewConnectionContext(db, hub, nil)
	hub.Subscribe(subscribed, [][]byte{[]byte("witness")})
	t.Cleanup(func() { hub.CleanupConnection(subscribed) })
	for _, tc := range []struct {
		name string
		ctx  *ConnectionContext
		args []string
		want string
	}{
		{"plain", plain, nil, "+PONG\r\n"},
		{"plain echo", plain, []string{"probe"}, "$5\r\nprobe\r\n"},
		{"subscribed", subscribed, nil, "*2\r\n$4\r\npong\r\n$0\r\n\r\n"},
		{"subscribed echo", subscribed, []string{"probe"}, "*2\r\n$4\r\npong\r\n$5\r\nprobe\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database.LockSharedExecution()
			finished := make(chan string, 1)
			go func() { finished <- dispatchTestCommand(tc.ctx, manager, "PING", tc.args...) }()
			select {
			case reply := <-finished:
				database.UnlockSharedExecution()
				if reply != tc.want {
					t.Fatalf("PING = %q, want %q", reply, tc.want)
				}
			case <-time.After(time.Second):
				database.UnlockSharedExecution()
				<-finished
				t.Fatal("PING waited for an unrelated ordinary write")
			}
		})
	}
	for _, tc := range []struct{ command, want string }{
		{"MULTI", "+OK\r\n"}, {"PING", "+QUEUED\r\n"}, {"EXEC", "*1\r\n+PONG\r\n"},
	} {
		if reply := dispatchTestCommand(plain, manager, tc.command); reply != tc.want {
			t.Fatalf("%s = %q, want %q", tc.command, reply, tc.want)
		}
	}
}
