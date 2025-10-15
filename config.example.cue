package example

template: {
    analogCell: {
        id: string
        type: "number"
        unit?: string
        ttl?: string
        scale?: number
        constant?: number
    }

    pid: {
        id: string
        #setpoint: string
        #process: string
        #output: string
        #kp: number
        #ki: number
        #kd: number
        #bias?: number
        #min?: number
        #max?: number

        type: "pid"
        inputs: [
            {
                id: "setpoint"
                cell: #setpoint
            },
            {
                id: "process"
                cell: #process
            },
        ]
        outputs: [
            {
                id: "output"
                cell: #output
            },
        ]
        settings: {
            kp: #kp
            ki: #ki
            kd: #kd
            if #bias != _|_ {
                bias: #bias
            }
            if #min != _|_ {
                min: #min
            }
            if #max != _|_ {
                max: #max
            }
        }
    }
}

config: {
    package: "example.plant"
    name: "Demo Plant"
    description: "CUE configuration showcasing Quarc features"
    cycle: "500ms"

    logging: {
        level: "info"
        format: "text"
        loki: {
            enabled: false
            url: ""
            labels: {}
        }
    }

    live_view: {
        heatmap: {
            cooldown: {
                cells: 12
                logic: 12
                programs: 12
            }
            colors: {
                read: "#4caf50"
                write: "#ef5350"
                stale: "#bdbdbd"
                logic: "#29b6f6"
                program: "#ab47bc"
                background: "#f5f5f5"
                border: "#424242"
            }
        }
    }

    telemetry: {
        enabled: false
    }

    helpers: [
        {
            name: "water_viscosity"
            arguments: ["temperature_c"]
            expression: """
                temperature_c <= 0 ? 0 : temperature_c * 0.001
            """
        },
    ]

    workers: {}

    dsl: {
        helpers: []
    }

    policies: {
        retry_backoff: "1s"
        retry_max: 3
        readback_verify: true
        watchdog_cell: "watchdog"
    }

    cells: [
        template.analogCell & {
            id: "raw_temperature"
            unit: "counts"
        },
        template.analogCell & {
            id: "temperature_c"
            unit: "°C"
        },
        template.analogCell & {
            id: "temperature_sum"
            unit: "°C·samples"
        },
        template.analogCell & {
            id: "temperature_mean"
            unit: "°C"
        },
        template.analogCell & {
            id: "temperature_queue"
            unit: "samples"
        },
        {
            id: "heater_enabled"
            type: "bool"
            constant: true
        },
        {
            id: "heater_command"
            type: "bool"
        },
        template.analogCell & {
            id: "pid_setpoint"
            unit: "°C"
            constant: 40
        },
        template.analogCell & {
            id: "pid_process"
            unit: "°C"
            constant: 35
        },
        template.analogCell & {
            id: "pid_output"
            unit: "%"
        },
        {
            id: "alarm_state"
            type: "bool"
            constant: false
        },
        {
            id: "sequencer_active"
            type: "string"
            constant: "Heat"
        },
        {
            id: "watchdog"
            type: "bool"
        },
    ]

    reads: [
        {
            id: "temperature_sensor"
            endpoint: {
                address: "192.168.10.10:502"
                unit_id: 1
                driver: "modbus"
                timeout: "2s"
            }
            function: "holding"
            start: 0
            length: 1
            ttl: "1s"
            signals: [
                {
                    cell: "raw_temperature"
                    offset: 0
                    type: "number"
                    scale: 0.1
                    aggregations: [
                        {
                            cell: "raw_temperature"
                        },
                    ]
                },
                {
                    cell: "temperature_sum"
                    offset: 0
                    type: "number"
                    scale: 0.1
                    buffer: {
                        capacity: 60
                    }
                    aggregations: [
                        {
                            cell: "temperature_sum"
                            aggregator: "sum"
                        },
                    ]
                },
                {
                    cell: "temperature_mean"
                    offset: 0
                    type: "number"
                    scale: 0.1
                    buffer: {
                        capacity: 60
                    }
                    aggregations: [
                        {
                            cell: "temperature_mean"
                            aggregator: "mean"
                        },
                    ]
                },
                {
                    cell: "temperature_queue"
                    offset: 0
                    type: "number"
                    scale: 0.1
                    buffer: {
                        capacity: 60
                    }
                    aggregations: [
                        {
                            cell: "temperature_queue"
                            aggregator: "queue_length"
                        },
                    ]
                },
            ]
        },
    ]

    programs: [
        {
            id: "temperature_ramp"
            type: "ramp"
            inputs: [
                {
                    id: "target"
                    cell: "pid_setpoint"
                },
            ]
            outputs: [
                {
                    id: "value"
                    cell: "pid_setpoint"
                },
            ]
            settings: {
                rate: 0.5
            }
        },
        template.pid & {
            id: "pid_temperature_controller"
            #setpoint: "pid_setpoint"
            #process: "pid_process"
            #output: "pid_output"
            #kp: 2.0
            #ki: 0.5
            #kd: 0.1
            #bias: 10
            #min: 0
            #max: 100
        },
    ]

    logic: [
        {
            id: "convert_temperature"
            target: "temperature_c"
            expression: """
                value("raw_temperature")
            """
            valid: """
                error == nil ? true : {"valid": false, "code": error_code, "message": error_message}
            """
            quality: """
                error == nil ? 0.9 : 0.0
            """
        },
        {
            id: "pid_measurement_feedback"
            target: "pid_process"
            expression: """
                value("temperature_c")
            """
            valid: """
                error == nil ? valid("temperature_c") : {"valid": false, "code": error_code, "message": error_message}
            """
            quality: """
                error == nil ? (valid("temperature_c") ? 0.95 : 0.0) : 0.0
            """
        },
        {
            id: "heater_control"
            target: "heater_command"
            expression: """
                !value("heater_enabled") ? false :
                value("alarm_state") ? false :
                value("pid_output") >= 60 ? true :
                value("pid_output") <= 40 ? false :
                heater_command
            """
            valid: """
                error == nil ? valid("pid_output") : {"valid": false, "code": error_code, "message": error_message}
            """
            quality: """
                error == nil ? (valid("pid_output") ? 0.95 : 0.0) : 0.0
            """
        },
        {
            id: "pid_setpoint_schedule"
            target: "pid_setpoint"
            expression: """
                value("sequencer_active") == "Heat" ? 65 :
                value("sequencer_active") == "Hold" ? 60 :
                value("sequencer_active") == "Drain" ? 45 :
                50
            """
        },
    ]

    writes: [
        {
            id: "heater_output"
            cell: "heater_command"
            endpoint: {
                address: "192.168.10.20:502"
                unit_id: 1
                driver: "modbus"
            }
            function: "coil"
            address: 5
            rate_limit: "2s"
            priority: 10
        },
    ]

    server: {
        enabled: true
        listen: ":15020"
        unit_id: 1
        cells: [
            {
                cell: "temperature_c"
                address: 0
                scale: 0.1
            },
            {
                cell: "heater_enabled"
                address: 1
            },
        ]
    }

    hot_reload: true
}
