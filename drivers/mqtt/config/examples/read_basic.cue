package examples

import "github.com/timzifer/quarc/drivers/mqtt/config/mqtt_driver"

// Basic QUARC read group overlay using the MQTT driver.
read_group: {
    id: "mqtt-temperatures"
    driver: {
        name: "mqtt"
        settings: mqtt_driver.ReadSettings & {
            connection: {
                broker: "tcp://broker:1883"
                client_id: "quarc-reader"
            }
            default_qos: 1
            buffer_defaults: {
                capacity: 32
            }
            subscriptions: [{
                topic: "site/temperature"
                cell: "cells.temperature"
                buffer: {
                    capacity: 32
                }
            }]
        }
    }
}
