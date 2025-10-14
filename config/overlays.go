package config

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"cuelang.org/go/cue/ast"
	"cuelang.org/go/cue/load"
)

var (
	overlayMu                   sync.RWMutex
	overlays                    = make(map[string]load.Source)
	defaultOverlayRegistrations []func() error
)

// OverlayDescriptor describes a virtual CUE file that can be registered as an overlay.
type OverlayDescriptor struct {
	Path   string
	Source load.Source
}

// RegisterOverlay registers a virtual CUE file that can be loaded via load.Config overlays.
func RegisterOverlay(path string, src load.Source) error {
	normalized, err := normalizeOverlayPath(path)
	if err != nil {
		return err
	}
	if src == nil {
		return errors.New("overlay source must not be nil")
	}
	overlayMu.Lock()
	defer overlayMu.Unlock()
	if _, exists := overlays[normalized]; exists {
		return fmt.Errorf("overlay %s already registered", normalized)
	}
	overlays[normalized] = src
	return nil
}

// RegisterOverlayString registers a virtual CUE file from a raw string.
func RegisterOverlayString(path, cue string) error {
	return RegisterOverlay(path, load.FromString(cue))
}

// RegisterOverlayFile registers a virtual CUE file from a parsed AST.
func RegisterOverlayFile(path string, file *ast.File) error {
	if file == nil {
		return errors.New("overlay file must not be nil")
	}
	return RegisterOverlay(path, load.FromFile(file))
}

func normalizeOverlayPath(path string) (string, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return "", errors.New("overlay path must not be empty")
	}
	cleaned := filepath.Clean(trimmed)
	if cleaned == "." || cleaned == string(filepath.Separator) {
		return "", errors.New("overlay path must reference a file")
	}
	return cleaned, nil
}

// ResolveOverlays returns a copy of the overlay registry with absolute paths for load.Config.
func ResolveOverlays(baseDir string) map[string]load.Source {
	overlayMu.RLock()
	defer overlayMu.RUnlock()
	if len(overlays) == 0 {
		return nil
	}
	resolved := make(map[string]load.Source, len(overlays))
	for path, src := range overlays {
		resolved[filepath.Join(baseDir, path)] = src
	}
	return resolved
}

// RegisterOverlayDescriptor registers an overlay described by the provided descriptor.
func RegisterOverlayDescriptor(desc OverlayDescriptor) error {
	return RegisterOverlay(desc.Path, desc.Source)
}

// RegisterOverlayDescriptors registers all provided overlay descriptors.
func RegisterOverlayDescriptors(descs ...OverlayDescriptor) error {
	for _, desc := range descs {
		if err := RegisterOverlayDescriptor(desc); err != nil {
			return err
		}
	}
	return nil
}

// ResetOverlaysForTest clears the overlay registry. This helper is intended for tests only.
func ResetOverlaysForTest() {
	overlayMu.Lock()
	overlays = make(map[string]load.Source)
	registrations := append([]func() error(nil), defaultOverlayRegistrations...)
	overlayMu.Unlock()

	for _, register := range registrations {
		if err := register(); err != nil {
			panic(fmt.Sprintf("restore default overlay: %v", err))
		}
	}
}

func resetOverlaysForTest() {
	ResetOverlaysForTest()
}

// RegisterDefaultOverlay registers an overlay that should persist across test resets.
func RegisterDefaultOverlay(register func() error) {
	if register == nil {
		panic("default overlay registration must not be nil")
	}
	if err := register(); err != nil {
		panic(fmt.Sprintf("register default overlay: %v", err))
	}
	overlayMu.Lock()
	defaultOverlayRegistrations = append(defaultOverlayRegistrations, register)
	overlayMu.Unlock()
}
