module github.com/timzifer/modbus_processor/drivers/modbus

go 1.24.3

require (
	github.com/goburrow/modbus v0.1.0
	github.com/rs/zerolog v1.34.0
	github.com/shopspring/decimal v1.4.0
	github.com/stretchr/testify v1.8.4
	github.com/timzifer/modbus_processor v0.0.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/goburrow/serial v0.1.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
)

replace github.com/timzifer/modbus_processor => ../..
