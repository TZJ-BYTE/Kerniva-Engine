//go:build !windows

package main

func platformFlags() func(string, string) error {
	return run
}
