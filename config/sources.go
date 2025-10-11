package config

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// SourceFiles returns the list of files that contributed configuration entries.
func SourceFiles(cfg *Config) []string {
	if cfg == nil {
		return nil
	}
	files := make(map[string]struct{})
	add := func(ref ModuleReference) {
		path := strings.TrimSpace(ref.File)
		if path == "" {
			return
		}
		if info, err := os.Stat(path); err == nil && info.IsDir() {
			return
		}
		abs, err := filepath.Abs(path)
		if err != nil {
			abs = path
		}
		files[abs] = struct{}{}
	}
	add(cfg.Source)
	for _, cell := range cfg.Cells {
		add(cell.Source)
	}
	for _, read := range cfg.Reads {
		add(read.Source)
	}
	for _, write := range cfg.Writes {
		add(write.Source)
	}
	for _, logic := range cfg.Logic {
		add(logic.Source)
	}
	for _, program := range cfg.Programs {
		add(program.Source)
	}
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}
