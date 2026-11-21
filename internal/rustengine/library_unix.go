//go:build linux || darwin

package rustengine

import "github.com/ebitengine/purego"

func openLibrary(path string) (uintptr, error) {
	return purego.Dlopen(path, purego.RTLD_NOW|purego.RTLD_LOCAL)
}

func findSymbol(library uintptr, name string) (uintptr, error) {
	return purego.Dlsym(library, name)
}

func closeLibrary(library uintptr) {
	_ = purego.Dlclose(library)
}
