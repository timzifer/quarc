package examples

import "github.com/quarc-labs/quarc_driver_mqtt/config/mqtt_driver"

// Example write target publishing to Home Assistant.
write_target: {
    id: "ha-garage-sensor"
    driver: "mqtt"
    cell: "cells.garage_temperature"
    driver_settings: mqtt_driver.WriteSettings & {
        connection: {
            broker: "tls://ha-broker:8883"
            client_id: "quarc-ha-writer"
        }
        topic: "home/garage/temperature/state"
        qos: 1
        retain: true
        payload: {
            encoding: "json"
        }
        home_assistant: {
            enabled: true
            discovery_prefix: "homeassistant"
            component: "sensor"
            object_id: "garage_temperature"
            name: "Garage Temperature"
            availability: {
                retain: true
            }
            device: {
                identifiers: ["garage-controller"]
                manufacturer: "QUARC Labs"
                model: "MQTT Bridge"
            }
        }
    }
}
