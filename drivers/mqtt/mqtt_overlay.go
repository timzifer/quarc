package mqtt

import "github.com/timzifer/quarc/config"

const mqttOverlayPath = "cue.mod/pkg/quarc.dev/quarc/drivers/mqtt/mqtt.cue"

const mqttOverlayContent = `package mqtt

import module "quarc.dev/quarc/module"

#Read: module.#Read & {
    driver?: {
        settings?: #ReadDriverSettings
        ...
    }
    ...
}

#Write: module.#Write & {
    driver?: {
        settings?: #WriteDriverSettings
        ...
    }
    ...
}

#PayloadConversion: {
    encoding?: "json" | "string" | "bytes" | "binary"
    value_type?: string
    path?: string
    format?: string
    properties?: [string]: string
}

#Auth: {
    username: string
    password: string
}

#TLS: {
    enabled: bool
    insecure_skip_verify?: bool
    ca_file?: string
    cert_file?: string
    key_file?: string
    server_name?: string
    alpn?: [...string]
}

#Will: {
    topic: string
    payload: _
    qos?: int & >=0 & <=2
    retain?: bool
    format?: #PayloadConversion
}

#Connection: {
    broker: string
    client_id?: string
    clean_session?: bool
    keep_alive?: string | int
    connect_timeout?: string | int
    auto_reconnect?: bool
    max_reconnect_interval?: string | int
    session_expiry?: string | int
    auth?: #Auth
    tls?: #TLS
    will?: #Will
    properties?: [string]: string
}

#BufferAggregation: {
    cell: string
    strategy?: string
    quality_cell?: string
    on_overflow?: string
}

#ReadBuffer: {
    capacity?: int & >0
    aggregations?: [...#BufferAggregation]
    on_overflow?: string
}

#ReadSubscription: {
    topic: string
    cell: string
    qos?: int & >=0 & <=2
    payload?: #PayloadConversion
    buffer?: #ReadBuffer
    home_assistant?: #HomeAssistant
}

#ReadDriverSettings: {
    connection: #Connection
    default_qos?: int & >=0 & <=2
    payload?: #PayloadConversion
    buffer_defaults?: #ReadBuffer
    subscriptions: [...#ReadSubscription]
}

#RateLimit: {
    min_interval?: string | int
}

#Deadband: {
    absolute?: number
    percent?: number
}

#HAAvailability: {
    topic?: string
    payload_online?: string
    payload_offline?: string
    retain?: bool
}

#HADevice: {
    identifiers?: [...string]
    manufacturer?: string
    model?: string
    name?: string
    sw_version?: string
    extra?: [string]: string
}

#HomeAssistant: {
    enabled: bool
    discovery_prefix?: string
    component: string
    object_id: string
    name?: string
    icon?: string
    state_topic?: string
    command_topic?: string
    state_template?: string
    availability?: #HAAvailability
    device?: #HADevice
    extra?: [string]: _
}

#WriteDriverSettings: {
    connection: #Connection
    topic: string
    qos?: int & >=0 & <=2
    retain?: bool
    payload?: #PayloadConversion
    deadband?: #Deadband
    rate_limit?: #RateLimit
    home_assistant?: #HomeAssistant
}
`

func init() {
	config.RegisterDefaultOverlay(func() error {
		return config.RegisterOverlayString(mqttOverlayPath, mqttOverlayContent)
	})
}
