package modbus

import "github.com/timzifer/quarc/config"

const modbusOverlayPath = "cue.mod/pkg/quarc.dev/quarc/drivers/modbus/modbus.cue"

const modbusOverlayContent = `package modbus

import module "quarc.dev/quarc/module"

#Read: module.#Read & {
    specification?: #ReadSpecification
    driver?: {
        settings?: #ReadSpecification
        ...
    }
    ...
}

#Write: module.#Write & {
    specification?: #WriteSpecification
    driver?: {
        settings?: #WriteSpecification
        ...
    }
    ...
}

#ReadSpecification: {
    function?: string
    start?: int & >=0 & <=65535
    length?: int & >=0 & <=65535
    ...
}

#WriteSpecification: {
    function?: string
    address?: int & >=0 & <=65535
    endianness?: string
    signed?: bool
    deadband?: number
    rate_limit?: string
    scale?: number
    ...
}

#ReadDriverSettings: #ReadSpecification

#WriteDriverSettings: #WriteSpecification
`

func init() {
	config.RegisterDefaultOverlay(func() error {
		return config.RegisterOverlayString(modbusOverlayPath, modbusOverlayContent)
	})
}
