package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/eventlog"
)

func platformFlags() func(string, string) error {
	name := flag.String("windows-service", "", "Windows SCM service name")
	library := flag.String("engine-library", "", "absolute native library path in Windows service mode")
	return func(configPath, stopFile string) error {
		if *name == "" {
			if *library != "" {
				return errors.New("--engine-library requires --windows-service")
			}
			return run(configPath, stopFile)
		}
		if stopFile != "" || !filepath.IsAbs(configPath) || !filepath.IsAbs(*library) {
			return errors.New("Windows service mode requires absolute --config and --engine-library paths and uses SCM for shutdown")
		}
		service, err := svc.IsWindowsService()
		if err != nil {
			return err
		}
		if !service {
			return errors.New("Windows service mode must be started by the Windows Service Control Manager")
		}
		return svc.Run(*name, &windowsService{name: *name, config: configPath, library: *library})
	}
}

type windowsService struct {
	name, config, library string
}

func (s *windowsService) prepare() error {
	localService, err := windows.CreateWellKnownSid(windows.WinLocalServiceSid)
	if err != nil {
		return err
	}
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return err
	}
	if !user.User.Sid.Equals(localService) {
		return errors.New("Windows service mode requires the LocalService account")
	}
	if err := windows.SetDefaultDllDirectories(windows.LOAD_LIBRARY_SEARCH_APPLICATION_DIR | windows.LOAD_LIBRARY_SEARCH_SYSTEM32); err != nil {
		return fmt.Errorf("restrict service DLL lookup: %w", err)
	}
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(strings.ToUpper(key), "KERNIVA_") {
			if err := os.Unsetenv(key); err != nil {
				return err
			}
		}
	}
	return os.Setenv("KERNIVA_ENGINE_LIBRARY", s.library)
}

func (s *windowsService) report(err error) (bool, uint32) {
	message := fmt.Sprintf("%s: %v", s.name, err)
	fmt.Fprintln(os.Stderr, message)
	// Operators may register an event source while installing the protected service.
	if log, openErr := eventlog.Open(s.name); openErr == nil {
		_ = log.Error(1, message)
		_ = log.Close()
	}
	return false, uint32(windows.ERROR_GEN_FAILURE)
}

func (s *windowsService) Execute(_ []string, requests <-chan svc.ChangeRequest, states chan<- svc.Status) (bool, uint32) {
	state := svc.Status{State: svc.StartPending, CheckPoint: 1, WaitHint: 30_000}
	states <- state
	if err := s.prepare(); err != nil {
		return s.report(err)
	}

	progress := time.NewTicker(5 * time.Second)
	defer progress.Stop()
	stop := make(chan struct{})
	ready := make(chan struct{})
	stopped := make(chan error, 1)
	go func() { stopped <- serve(s.config, "", stop, func() { close(ready) }) }()
	if err := monitorWindowsService(state, requests, states, stop, ready, stopped, progress.C); err != nil {
		return s.report(err)
	}
	return false, 0
}

func monitorWindowsService(state svc.Status, requests <-chan svc.ChangeRequest, states chan<- svc.Status,
	stop chan<- struct{}, ready <-chan struct{}, stopped <-chan error, progress <-chan time.Time,
) error {
	for {
		select {
		case <-progress:
			if state.State == svc.StartPending || state.State == svc.StopPending {
				state.CheckPoint++
				states <- state
			}
		case <-ready:
			ready = nil
			if state.State == svc.StartPending {
				state = svc.Status{State: svc.Running, Accepts: svc.AcceptStop | svc.AcceptShutdown}
				states <- state
			}
		case change, ok := <-requests:
			if !ok {
				requests = nil
			}
			if !ok || change.Cmd == svc.Stop || change.Cmd == svc.Shutdown {
				if state.State != svc.StopPending {
					state = svc.Status{State: svc.StopPending, CheckPoint: 1, WaitHint: 30_000}
					states <- state
					close(stop)
				}
			} else if change.Cmd == svc.Interrogate {
				states <- state
			}
		case err := <-stopped:
			return err
		}
	}
}
