//go:build !windows

package main

import "os/exec"

func prepareChild(*exec.Cmd) {}
