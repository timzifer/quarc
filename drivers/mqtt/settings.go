package mqtt

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/timzifer/quarc/config"
)

// ConnectionSettings describe how to reach the MQTT broker shared by readers and writers.
type ConnectionSettings struct {
	Broker           string            `json:"broker"`
	ClientID         string            `json:"client_id,omitempty"`
	CleanSession     *bool             `json:"clean_session,omitempty"`
	KeepAlive        *config.Duration  `json:"keep_alive,omitempty"`
	ConnectTimeout   *config.Duration  `json:"connect_timeout,omitempty"`
	AutoReconnect    *bool             `json:"auto_reconnect,omitempty"`
	MaxReconnect     *config.Duration  `json:"max_reconnect_interval,omitempty"`
	Auth             *AuthSettings     `json:"auth,omitempty"`
	TLS              *TLSSettings      `json:"tls,omitempty"`
	Will             *WillSettings     `json:"will,omitempty"`
	SessionExpiry    *config.Duration  `json:"session_expiry,omitempty"`
	CustomProperties map[string]string `json:"properties,omitempty"`
}

// AuthSettings capture username/password authentication for MQTT.
type AuthSettings struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// TLSSettings allow TLS connections to be configured.
type TLSSettings struct {
	Enabled            bool     `json:"enabled"`
	InsecureSkipVerify bool     `json:"insecure_skip_verify"`
	CAFile             string   `json:"ca_file,omitempty"`
	CertFile           string   `json:"cert_file,omitempty"`
	KeyFile            string   `json:"key_file,omitempty"`
	ServerName         string   `json:"server_name,omitempty"`
	ALPN               []string `json:"alpn,omitempty"`
}

// WillSettings describe a last will message for the MQTT client.
type WillSettings struct {
	Topic   string             `json:"topic"`
	Payload json.RawMessage    `json:"payload"`
	QoS     *byte              `json:"qos,omitempty"`
	Retain  *bool              `json:"retain,omitempty"`
	Format  *PayloadConversion `json:"format,omitempty"`
}

// PayloadConversion defines how payloads are encoded or decoded.
type PayloadConversion struct {
	Encoding   string            `json:"encoding,omitempty"`
	ValueType  string            `json:"value_type,omitempty"`
	Path       string            `json:"path,omitempty"`
	Format     string            `json:"format,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

// ReadSettings encapsulates driver_settings for a read group.
type ReadSettings struct {
	Connection     ConnectionSettings  `json:"connection"`
	DefaultQoS     *byte               `json:"default_qos,omitempty"`
	Payload        *PayloadConversion  `json:"payload,omitempty"`
	Subscriptions  []ReadSubscription  `json:"subscriptions"`
	BufferDefaults *ReadBufferSettings `json:"buffer_defaults,omitempty"`
}

// ReadSubscription binds an MQTT topic to a QUARC cell.
type ReadSubscription struct {
	Topic         string                `json:"topic"`
	Cell          string                `json:"cell"`
	QoS           *byte                 `json:"qos,omitempty"`
	Payload       *PayloadConversion    `json:"payload,omitempty"`
	Buffer        *ReadBufferSettings   `json:"buffer,omitempty"`
	HomeAssistant *HomeAssistantOptions `json:"home_assistant,omitempty"`
}

// ReadBufferSettings describe optional overrides for signal buffer behaviour.
type ReadBufferSettings struct {
	Capacity     *int                `json:"capacity,omitempty"`
	Aggregations []BufferAggregation `json:"aggregations,omitempty"`
	OnOverflow   string              `json:"on_overflow,omitempty"`
}

// BufferAggregation maps a buffered signal aggregation to a target cell.
type BufferAggregation struct {
	Cell        string `json:"cell"`
	Strategy    string `json:"strategy,omitempty"`
	QualityCell string `json:"quality_cell,omitempty"`
	OnOverflow  string `json:"on_overflow,omitempty"`
}

// Deadband ensures only significant changes trigger a publish.
type Deadband struct {
	Absolute *float64 `json:"absolute,omitempty"`
	Percent  *float64 `json:"percent,omitempty"`
}

// RateLimit limits how frequently a value may be published.
type RateLimit struct {
	MinInterval config.Duration `json:"min_interval,omitempty"`
}

// WriteSettings encapsulates driver_settings for a write target.
type WriteSettings struct {
	Connection    ConnectionSettings    `json:"connection"`
	Topic         string                `json:"topic"`
	QoS           *byte                 `json:"qos,omitempty"`
	Retain        *bool                 `json:"retain,omitempty"`
	Payload       *PayloadConversion    `json:"payload,omitempty"`
	Deadband      *Deadband             `json:"deadband,omitempty"`
	RateLimit     *RateLimit            `json:"rate_limit,omitempty"`
	HomeAssistant *HomeAssistantOptions `json:"home_assistant,omitempty"`
}

// HomeAssistantOptions configure discovery payloads and availability state for MQTT writers.
type HomeAssistantOptions struct {
	Enabled         bool            `json:"enabled"`
	DiscoveryPrefix string          `json:"discovery_prefix,omitempty"`
	Component       string          `json:"component,omitempty"`
	ObjectID        string          `json:"object_id,omitempty"`
	Name            string          `json:"name,omitempty"`
	Icon            string          `json:"icon,omitempty"`
	StateTopic      string          `json:"state_topic,omitempty"`
	CommandTopic    string          `json:"command_topic,omitempty"`
	Availability    *HAAvailability `json:"availability,omitempty"`
	Device          *HADevice       `json:"device,omitempty"`
	Extra           map[string]any  `json:"extra,omitempty"`
	StateTemplate   string          `json:"state_template,omitempty"`
}

// HAAvailability configures the topics used to report availability to Home Assistant.
type HAAvailability struct {
	Topic          string `json:"topic,omitempty"`
	PayloadOnline  string `json:"payload_online,omitempty"`
	PayloadOffline string `json:"payload_offline,omitempty"`
	Retain         bool   `json:"retain,omitempty"`
}

// HADevice describes a device entry for Home Assistant discovery.
type HADevice struct {
	Identifiers  []string          `json:"identifiers,omitempty"`
	Manufacturer string            `json:"manufacturer,omitempty"`
	Model        string            `json:"model,omitempty"`
	Name         string            `json:"name,omitempty"`
	SWVersion    string            `json:"sw_version,omitempty"`
	Extra        map[string]string `json:"extra,omitempty"`
}

// ResolveQoS picks the per-subscription QoS or falls back to the defaults.
func (s ReadSettings) ResolveQoS(sub ReadSubscription) byte {
	if sub.QoS != nil {
		return *sub.QoS
	}
	if s.DefaultQoS != nil {
		return *s.DefaultQoS
	}
	return 0
}

// ResolvePayload obtains the payload conversion for a subscription or write target.
func (s ReadSettings) ResolvePayload(sub ReadSubscription) PayloadConversion {
	if sub.Payload != nil {
		return *sub.Payload
	}
	if s.Payload != nil {
		return *s.Payload
	}
	return PayloadConversion{Encoding: "json"}
}

// ResolveWriterPayload returns the conversion configuration for a write target.
func (s WriteSettings) ResolveWriterPayload() PayloadConversion {
	if s.Payload != nil {
		return *s.Payload
	}
	return PayloadConversion{Encoding: "json"}
}

// RetainFlag resolves the retain behaviour for a writer.
func (s WriteSettings) RetainFlag() bool {
	if s.Retain == nil {
		return false
	}
	return *s.Retain
}

// QoSLevel resolves the QoS for a writer publication.
func (s WriteSettings) QoSLevel() byte {
	if s.QoS == nil {
		return 0
	}
	return *s.QoS
}

// DurationValue converts a config.Duration pointer to time.Duration.
func DurationValue(d *config.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return d.Duration
}

// Validate performs lightweight validation of read settings.
//
// When requireConnection is false the caller is expected to provide a shared
// connection handle instead of inline broker settings.
func (s ReadSettings) Validate(requireConnection bool) error {
	if requireConnection && s.Connection.Broker == "" {
		return fmt.Errorf("connection.broker is required")
	}
	if len(s.Subscriptions) == 0 {
		return fmt.Errorf("at least one subscription must be configured")
	}
	for i, sub := range s.Subscriptions {
		if sub.Topic == "" {
			return fmt.Errorf("subscription %d missing topic", i)
		}
		if sub.Cell == "" {
			return fmt.Errorf("subscription %d missing cell", i)
		}
		if sub.HomeAssistant != nil && sub.HomeAssistant.Enabled {
			if sub.HomeAssistant.Component == "" {
				return fmt.Errorf("subscription %d home_assistant.component is required when enabled", i)
			}
			if sub.HomeAssistant.ObjectID == "" {
				return fmt.Errorf("subscription %d home_assistant.object_id is required when enabled", i)
			}
		}
	}
	return nil
}

// Validate performs lightweight validation of write settings.
//
// When requireConnection is false the caller is expected to provide a shared
// connection handle instead of inline broker settings.
func (s WriteSettings) Validate(requireConnection bool) error {
	if requireConnection && s.Connection.Broker == "" {
		return fmt.Errorf("connection.broker is required")
	}
	if s.Topic == "" {
		return fmt.Errorf("topic must be provided")
	}
	if s.HomeAssistant != nil && s.HomeAssistant.Enabled {
		if s.HomeAssistant.Component == "" {
			return fmt.Errorf("home_assistant.component is required when enabled")
		}
		if s.HomeAssistant.ObjectID == "" {
			return fmt.Errorf("home_assistant.object_id is required when enabled")
		}
	}
	return nil
}
