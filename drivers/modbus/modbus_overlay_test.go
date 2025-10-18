package modbus

import (
	"path/filepath"
	"testing"

	"github.com/timzifer/quarc/config"
)

func TestModbusOverlayRegistered(t *testing.T) {
	config.ResetOverlaysForTest()
	t.Cleanup(config.ResetOverlaysForTest)

	overlays := config.ResolveOverlays(".")
	path := filepath.Join(".", modbusOverlayPath)
	if _, ok := overlays[path]; !ok {
		t.Fatalf("modbus overlay %q not registered", path)
	}
}
