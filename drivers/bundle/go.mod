module github.com/timzifer/modbus_processor/drivers/bundle

go 1.24.3

require (
	github.com/timzifer/modbus_processor v0.0.0
	github.com/timzifer/modbus_processor/drivers/canstream v0.0.0
	github.com/timzifer/modbus_processor/drivers/modbus v0.0.0
)

require (
	github.com/expr-lang/expr v1.16.5 // indirect
	github.com/goburrow/modbus v0.1.0 // indirect
	github.com/goburrow/serial v0.1.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/rs/zerolog v1.34.0 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	go.einride.tech/can v0.16.1 // indirect
	golang.org/x/sys v0.31.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/timzifer/modbus_processor => ../..

replace github.com/timzifer/modbus_processor/drivers/canstream => ../canstream

replace github.com/timzifer/modbus_processor/drivers/modbus => ../modbus
