module github.com/timzifer/modbus_processor/drivers/canstream

go 1.24.3

require (
	github.com/rs/zerolog v1.34.0
	github.com/timzifer/modbus_processor v0.0.0
	go.einride.tech/can v0.16.1
)

require (
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	golang.org/x/sys v0.31.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/timzifer/modbus_processor => ../..
