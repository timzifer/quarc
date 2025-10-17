package examples

import "github.com/quarc-labs/quarc_driver_mqtt/config/mqtt_driver"

// Example QUARC read group exposing a cell to Home Assistant.
read_group: {
    id: "ha-setpoint"
    driver: {
        name: "mqtt"
        settings: mqtt_driver.ReadSettings & {
            connection: {
                broker: "tcp://ha-broker:1883"
                client_id: "quarc-ha-reader"
            }
            subscriptions: [{
                topic: "home/hvac/setpoint/set"
                cell: "cells.hvac_setpoint"
                home_assistant: {
                    enabled: true
                    component: "number"
                    object_id: "hvac_setpoint"
                    command_topic: "home/hvac/setpoint/set"
                    state_topic: "home/hvac/setpoint/state"
                    name: "HVAC Setpoint"
                }
            }]
        }
    }
}
