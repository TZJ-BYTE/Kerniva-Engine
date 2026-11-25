package main

import (
	"os/exec"
	"syscall"
)

func prepareChild(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
}
