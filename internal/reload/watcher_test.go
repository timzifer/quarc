package reload

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/timzifer/quarc/config"
)

func TestUniquePathsFiltersDuplicatesAndEmptyValues(t *testing.T) {
	paths := []string{"", "/tmp/a", "/tmp/b", "/tmp/a", "\t", "/tmp/c", "/tmp/b"}
	got := uniquePaths(paths)
	want := []string{"/tmp/a", "/tmp/b", "/tmp/c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("uniquePaths() = %v, want %v", got, want)
	}
}

func TestWatcherUpdateIncludesExistingFilesAndRoot(t *testing.T) {
	dir := t.TempDir()
	configFile := filepath.Join(dir, "config.cue")
	cellFile := filepath.Join(dir, "cells.cue")
	rootFile := filepath.Join(dir, "quarc.cue")

	writeFile(t, configFile, "config")
	writeFile(t, cellFile, "cell")
	writeFile(t, rootFile, "root")

	cfg := &config.Config{
		Source: config.ModuleReference{File: configFile},
		Cells:  []config.CellConfig{{Source: config.ModuleReference{File: cellFile}}},
	}

	var watcher Watcher
	if err := watcher.Update(rootFile, cfg); err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	if len(watcher.files) != 3 {
		t.Fatalf("expected 3 tracked files, got %d", len(watcher.files))
	}
	if _, ok := watcher.files[configFile]; !ok {
		t.Fatalf("config file %s not tracked", configFile)
	}
	if _, ok := watcher.files[cellFile]; !ok {
		t.Fatalf("cell file %s not tracked", cellFile)
	}
	if _, ok := watcher.files[rootFile]; !ok {
		t.Fatalf("root file %s not tracked", rootFile)
	}
}

func TestWatcherUpdateSkipsMissingFiles(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.cue")

	cfg := &config.Config{
		Source: config.ModuleReference{File: missing},
	}

	var watcher Watcher
	if err := watcher.Update("", cfg); err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	if len(watcher.files) != 0 {
		t.Fatalf("expected 0 tracked files, got %d", len(watcher.files))
	}
}

func TestWatcherCheckDetectsChangesAndRemovals(t *testing.T) {
	dir := t.TempDir()
	fileA := filepath.Join(dir, "a.cue")
	fileB := filepath.Join(dir, "b.cue")
	writeFile(t, fileA, "first")
	writeFile(t, fileB, "second")

	cfg := &config.Config{
		Source: config.ModuleReference{File: fileA},
		Writes: []config.WriteTargetConfig{{Source: config.ModuleReference{File: fileB}}},
	}

	watcher, err := NewWatcher("", cfg)
	if err != nil {
		t.Fatalf("NewWatcher() error = %v", err)
	}

	if changed, err := watcher.Check(); err != nil {
		t.Fatalf("Check() error = %v", err)
	} else if len(changed) != 0 {
		t.Fatalf("expected no changes on first check, got %v", changed)
	}

	time.Sleep(10 * time.Millisecond)
	writeFile(t, fileA, "first-UPDATED")
	if err := os.Remove(fileB); err != nil {
		t.Fatalf("Remove(%s) error = %v", fileB, err)
	}

	changed, err := watcher.Check()
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}

	sort.Strings(changed)
	expected := []string{fileA, fileB}
	sort.Strings(expected)
	if !reflect.DeepEqual(changed, expected) {
		t.Fatalf("Check() = %v, want %v", changed, expected)
	}
}

func TestWatcherHandlesNilReceiver(t *testing.T) {
	var watcher *Watcher
	if err := watcher.Update("", &config.Config{}); err != nil {
		t.Fatalf("nil watcher Update() error = %v", err)
	}
	if changed, err := watcher.Check(); err != nil {
		t.Fatalf("nil watcher Check() error = %v", err)
	} else if changed != nil {
		t.Fatalf("expected nil slice from nil watcher, got %v", changed)
	}
}

func writeFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", path, err)
	}
}
