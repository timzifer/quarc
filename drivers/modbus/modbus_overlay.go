package modbus

import "github.com/timzifer/quarc/config"

const modbusOverlayPath = "cue.mod/pkg/quarc.dev/quarc/drivers/modbus/modbus.cue"

const modbusOverlayContent = `package modbus

import module "quarc.dev/quarc/module"

#Read: module.#Read & {
    driver_settings?: #ReadDriverSettings
    ...
}

#Write: module.#Write & {
    driver_settings?: #WriteDriverSettings
    ...
}

#ReadDriverSettings: {
    function?: string
    start?: int & >=0 & <=65535
    length?: int & >=0 & <=65535
    ...
}

#WriteDriverSettings: {
    function?: string
    address?: int & >=0 & <=65535
    endianness?: string
    signed?: bool
    deadband?: number
    rate_limit?: string
    scale?: number
    ...
}
`

func init() {
	config.RegisterDefaultOverlay(func() error {
		return config.RegisterOverlayString(modbusOverlayPath, modbusOverlayContent)
	})
}
