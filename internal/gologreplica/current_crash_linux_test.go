//go:build linux

package gologreplica

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

const currentCrashFixtureEnv = "KERNIVA_REPLICA_CURRENT_CRASH_FIXTURE"

type currentCrashFixtureConfig struct {
	Root     string
	Upstream string
	Stage    currentPublishStage
}

func TestReplicaRecoversAtCurrentPublishBoundaries(t *testing.T) {
	if os.Getenv("KERNIVA_ENGINE_LIBRARY") == "" {
		t.Skip("set KERNIVA_ENGINE_LIBRARY to the built Linux recovery library")
	}
	for _, test := range []struct {
		name  string
		stage currentPublishStage
	}{
		{"before_rename", currentBeforeRename},
		{"after_directory_sync", currentAfterDirectorySync},
	} {
		t.Run(test.name, func(t *testing.T) {
			primary := t.TempDir()
			primaryDB := filepath.Join(primary, dbName(0))
			writer := openWriter(t, primaryDB)
			put(t, writer, "updated", "old value", 0)
			put(t, writer, "deleted", "old entry", 0)
			check(t, writer.Close())
			handler, err := NewSource(primary)
			check(t, err)
			source := httptest.NewServer(handler)
			t.Cleanup(source.Close)

			root := t.TempDir()
			follower, err := OpenFollower(root, source.URL, []int{0})
			check(t, err)
			t.Cleanup(func() { _ = follower.Close() })
			poll(t, follower)
			directory := follower.byNumber[0].directory
			check(t, follower.Close())
			pointerPath := filepath.Join(directory, "CURRENT")
			old := readCrashTestPointer(t, pointerPath)
			if old.Sequence != 2 || old.Snapshot != 0 || !validSource(old.Source) || !validGeneration(old.Generation) {
				t.Fatalf("unexpected seed generation: %+v", old)
			}

			writer = openWriter(t, primaryDB)
			put(t, writer, "updated", "new value", 0)
			check(t, writer.Delete([]byte("deleted")))
			put(t, writer, "added", "new entry", 0)
			check(t, writer.Close())
			crashAtCurrentPublish(t, currentCrashFixtureConfig{Root: root, Upstream: source.URL, Stage: test.stage})

			current := readCrashTestPointer(t, pointerPath)
			temporary, err := filepath.Glob(filepath.Join(directory, ".CURRENT-*"))
			check(t, err)
			staged := current
			if test.stage == currentBeforeRename {
				if current != old || len(temporary) != 1 {
					t.Fatalf("pre-rename crash left CURRENT %+v and staged pointers %v", current, temporary)
				}
				staged = readCrashTestPointer(t, temporary[0])
			} else if len(temporary) != 0 {
				t.Fatalf("synced rename left staged pointers %v", temporary)
			}
			if staged.Sequence != 5 || staged.Source != old.Source || staged.Snapshot != 0 ||
				!validGeneration(staged.Generation) || staged.Generation == old.Generation {
				t.Fatalf("unexpected staged generation after crash: %+v", staged)
			}
			for _, state := range []installed{old, staged} {
				info, err := os.Stat(filepath.Join(directory, state.Generation))
				check(t, err)
				if !info.IsDir() {
					t.Fatalf("generation %q is not a directory", state.Generation)
				}
			}

			source.Close()
			reopened, err := OpenFollower(root, source.URL, []int{0})
			check(t, err)
			defer reopened.Close()
			want := Status{Ready: true, Source: old.Source, Sequence: current.Sequence}
			if got := reportStatus(t, reopened, 0); got != want {
				t.Fatalf("offline reopened status %+v, want %+v", got, want)
			}
			if test.stage == currentBeforeRename {
				expectGet(t, reopened, 0, "updated", http.StatusOK, "old value")
				expectGet(t, reopened, 0, "deleted", http.StatusOK, "old entry")
				expectGet(t, reopened, 0, "added", http.StatusNotFound, "")
			} else {
				expectGet(t, reopened, 0, "updated", http.StatusOK, "new value")
				expectGet(t, reopened, 0, "deleted", http.StatusNotFound, "")
				expectGet(t, reopened, 0, "added", http.StatusOK, "new entry")
			}
			generations, err := filepath.Glob(filepath.Join(directory, "generation-*"))
			check(t, err)
			temporary, err = filepath.Glob(filepath.Join(directory, ".CURRENT-*"))
			check(t, err)
			if len(generations) != 1 || filepath.Base(generations[0]) != current.Generation || len(temporary) != 0 {
				t.Fatalf("restart retained generations %v and staged pointers %v, want only %s",
					generations, temporary, current.Generation)
			}
		})
	}
}

func readCrashTestPointer(t *testing.T, path string) installed {
	t.Helper()
	raw, err := os.ReadFile(path)
	check(t, err)
	var result installed
	check(t, json.Unmarshal(raw, &result))
	return result
}

func crashAtCurrentPublish(t *testing.T, cfg currentCrashFixtureConfig) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	marker, childMarker, err := os.Pipe()
	check(t, err)
	defer marker.Close()
	defer childMarker.Close()
	childGate, gate, err := os.Pipe()
	check(t, err)
	defer childGate.Close()
	defer gate.Close()
	deadline, _ := ctx.Deadline()
	check(t, marker.SetReadDeadline(deadline))

	self, err := os.Executable()
	check(t, err)
	config, err := json.Marshal(cfg)
	check(t, err)
	fixture := exec.CommandContext(ctx, self, "-test.run=^TestReplicaCurrentCrashFixture$", "-test.timeout=25s")
	fixture.Env = append(os.Environ(), currentCrashFixtureEnv+"="+string(config))
	fixture.ExtraFiles = []*os.File{childMarker, childGate}
	var output bytes.Buffer
	fixture.Stdout, fixture.Stderr = &output, &output
	check(t, fixture.Start())
	defer func() {
		if fixture.ProcessState == nil {
			cancel()
			_ = fixture.Wait()
		}
	}()
	check(t, childMarker.Close())
	check(t, childGate.Close())

	var stage [1]byte
	_, markerErr := io.ReadFull(marker, stage[:])
	// CommandContext kills the subprocess on cancellation. Keep the gate held
	// until it has exited so publish cannot resume between the marker and SIGKILL.
	cancel()
	waitErr := fixture.Wait()
	if markerErr != nil || currentPublishStage(stage[0]) != cfg.Stage {
		t.Fatalf("reach CURRENT checkpoint %d: marker=%d, read=%v, exit=%v\n%s",
			cfg.Stage, stage[0], markerErr, waitErr, output.String())
	}
	var exit *exec.ExitError
	if !errors.As(waitErr, &exit) {
		t.Fatalf("CURRENT fixture should die from SIGKILL, got %v\n%s", waitErr, output.String())
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("CURRENT fixture should die from SIGKILL, got %v\n%s", waitErr, output.String())
	}
}

func TestReplicaCurrentCrashFixture(t *testing.T) {
	config, ok := os.LookupEnv(currentCrashFixtureEnv)
	if !ok {
		t.Skip("started by the CURRENT publish boundary test")
	}
	var cfg currentCrashFixtureConfig
	check(t, json.Unmarshal([]byte(config), &cfg))
	follower, err := OpenFollower(cfg.Root, cfg.Upstream, []int{0})
	check(t, err)
	defer follower.Close()
	marker := os.NewFile(3, "CURRENT publish checkpoint")
	defer marker.Close()
	gate := os.NewFile(4, "CURRENT publish parent gate")
	defer gate.Close()
	follower.byNumber[0].onCurrentPublish = func(stage currentPublishStage) {
		if stage != cfg.Stage {
			return
		}
		_, err := marker.Write([]byte{byte(stage)})
		check(t, err)
		var released [1]byte
		_, err = gate.Read(released[:])
		t.Fatalf("CURRENT fixture reached checkpoint %d but the parent gate released: %v", stage, err)
	}
	poll(t, follower)
	t.Fatalf("CURRENT fixture did not stop at checkpoint %d", cfg.Stage)
}
