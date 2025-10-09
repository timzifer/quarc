package reload

import (
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/timzifer/modbus_processor/internal/config"
)

type fileState struct {
	modTime time.Time
	size    int64
}

// Watcher keeps track of configuration source files and detects modifications.
type Watcher struct {
	mu    sync.Mutex
	files map[string]fileState
}

// NewWatcher builds a watcher with the known files from the configuration.
func NewWatcher(root string, cfg *config.Config) (*Watcher, error) {
	watcher := &Watcher{}
	if err := watcher.Update(root, cfg); err != nil {
		return nil, err
	}
	return watcher, nil
}

// Update rebuilds the tracked file list from the provided configuration.
func (w *Watcher) Update(root string, cfg *config.Config) error {
	if w == nil {
		return nil
	}
	paths := config.SourceFiles(cfg)
	if root != "" {
		abs, err := filepath.Abs(root)
		if err == nil {
			if info, err := os.Stat(abs); err == nil && !info.IsDir() {
				paths = append(paths, abs)
			}
		}
	}
	states := make(map[string]fileState, len(paths))
	for _, path := range uniquePaths(paths) {
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		if info.IsDir() {
			continue
		}
		states[path] = fileState{modTime: info.ModTime(), size: info.Size()}
	}
	w.mu.Lock()
	w.files = states
	w.mu.Unlock()
	return nil
}

// Check reports the files that changed since the last snapshot.
func (w *Watcher) Check() ([]string, error) {
	if w == nil {
		return nil, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	changed := make([]string, 0)
	for path, state := range w.files {
		info, err := os.Stat(path)
		if err != nil {
			changed = append(changed, path)
			continue
		}
		if info.IsDir() {
			continue
		}
		if info.ModTime().After(state.modTime) || info.Size() != state.size {
			changed = append(changed, path)
		}
	}
	sort.Strings(changed)
	return changed, nil
}

func uniquePaths(paths []string) []string {
	seen := make(map[string]struct{}, len(paths))
	result := make([]string, 0, len(paths))
	for _, path := range paths {
		if path == "" {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		result = append(result, path)
	}
	return result
}
