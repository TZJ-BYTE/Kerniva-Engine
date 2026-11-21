//go:build windows

package rustengine

import "syscall"

func openLibrary(path string) (uintptr, error) {
	library, err := syscall.LoadLibrary(path)
	return uintptr(library), err
}

func findSymbol(library uintptr, name string) (uintptr, error) {
	return syscall.GetProcAddress(syscall.Handle(library), name)
}

func closeLibrary(library uintptr) {
	_ = syscall.FreeLibrary(syscall.Handle(library))
}
