package mqtt

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
)

type homeAssistantTopicDefaults struct {
	state   string
	command string
}

type homeAssistantPublisher struct {
	opts      *HomeAssistantOptions
	mainTopic string

	discoveryTopic   string
	discoveryPayload []byte

	availabilityTopic   string
	availabilityOnline  string
	availabilityOffline string
	availabilityRetain  bool

	stateTopic    string
	stateTemplate string
	commandTopic  string

	once sync.Once
}

func newHomeAssistantPublisher(opts *HomeAssistantOptions, conn ConnectionSettings, mainTopic string, defaults homeAssistantTopicDefaults) (*homeAssistantPublisher, error) {
	if opts == nil || !opts.Enabled {
		return nil, nil
	}
	prefix := strings.Trim(opts.DiscoveryPrefix, "/")
	if prefix == "" {
		prefix = "homeassistant"
	}
	discoveryTopic := fmt.Sprintf("%s/%s/%s/config", prefix, opts.Component, opts.ObjectID)
	stateTopic := opts.StateTopic
	if stateTopic == "" {
		stateTopic = defaults.state
	}
	commandTopic := opts.CommandTopic
	if commandTopic == "" {
		commandTopic = defaults.command
	}
	availabilityTopic := ""
	availabilityOnline := "online"
	availabilityOffline := "offline"
	availabilityRetain := true
	if opts.Availability != nil {
		if opts.Availability.Topic != "" {
			availabilityTopic = opts.Availability.Topic
		}
		if opts.Availability.PayloadOnline != "" {
			availabilityOnline = opts.Availability.PayloadOnline
		}
		if opts.Availability.PayloadOffline != "" {
			availabilityOffline = opts.Availability.PayloadOffline
		}
		availabilityRetain = opts.Availability.Retain
	}
	if availabilityTopic == "" {
		clientID := conn.ClientID
		if clientID == "" {
			clientID = opts.ObjectID
		}
		availabilityTopic = fmt.Sprintf("%s/status/%s", prefix, clientID)
	}

	payload := map[string]any{
		"name":      opts.Name,
		"object_id": opts.ObjectID,
		"unique_id": uniqueID(conn.ClientID, opts.ObjectID),
	}
	if stateTopic != "" {
		payload["state_topic"] = stateTopic
	}
	if opts.Icon != "" {
		payload["icon"] = opts.Icon
	}
	if opts.StateTemplate != "" {
		payload["value_template"] = opts.StateTemplate
	}
	if commandTopic != "" {
		payload["command_topic"] = commandTopic
	}
	if opts.Device != nil {
		device := map[string]any{}
		if len(opts.Device.Identifiers) > 0 {
			device["identifiers"] = opts.Device.Identifiers
		}
		if opts.Device.Manufacturer != "" {
			device["manufacturer"] = opts.Device.Manufacturer
		}
		if opts.Device.Model != "" {
			device["model"] = opts.Device.Model
		}
		if opts.Device.Name != "" {
			device["name"] = opts.Device.Name
		}
		if opts.Device.SWVersion != "" {
			device["sw_version"] = opts.Device.SWVersion
		}
		if len(opts.Device.Extra) > 0 {
			for k, v := range opts.Device.Extra {
				device[k] = v
			}
		}
		payload["device"] = device
	}
	if len(opts.Extra) > 0 {
		for k, v := range opts.Extra {
			payload[k] = v
		}
	}
	if availabilityTopic != "" {
		payload["availability_topic"] = availabilityTopic
		payload["payload_available"] = availabilityOnline
		payload["payload_not_available"] = availabilityOffline
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("mqtt: encode home assistant discovery: %w", err)
	}

	return &homeAssistantPublisher{
		opts:                opts,
		mainTopic:           mainTopic,
		discoveryTopic:      discoveryTopic,
		discoveryPayload:    body,
		availabilityTopic:   availabilityTopic,
		availabilityOnline:  availabilityOnline,
		availabilityOffline: availabilityOffline,
		availabilityRetain:  availabilityRetain,
		stateTopic:          stateTopic,
		stateTemplate:       opts.StateTemplate,
		commandTopic:        commandTopic,
	}, nil
}

func (h *homeAssistantPublisher) Ensure(client mqtt.Client, logger zerolog.Logger) {
	if h == nil {
		return
	}
	h.once.Do(func() {
		token := client.Publish(h.discoveryTopic, 1, true, h.discoveryPayload)
		if token.Wait() && token.Error() != nil {
			logger.Error().Err(token.Error()).Str("topic", h.discoveryTopic).Msg("mqtt: home assistant discovery publish failed")
		} else {
			logger.Info().Str("topic", h.discoveryTopic).Msg("mqtt: home assistant discovery published")
		}
	})
}

func (h *homeAssistantPublisher) PublishAvailability(client mqtt.Client, logger zerolog.Logger, online bool) {
	if h == nil || h.availabilityTopic == "" {
		return
	}
	payload := h.availabilityOffline
	if online {
		payload = h.availabilityOnline
	}
	token := client.Publish(h.availabilityTopic, 1, h.availabilityRetain, payload)
	if token.Wait() && token.Error() != nil {
		logger.Error().Err(token.Error()).Str("topic", h.availabilityTopic).Msg("mqtt: home assistant availability publish failed")
	}
}

func (h *homeAssistantPublisher) PublishMirroredState(client mqtt.Client, logger zerolog.Logger, payload []byte, qos byte, retain bool) {
	if h == nil {
		return
	}
	if h.stateTopic == "" || h.stateTopic == h.mainTopic {
		return
	}
	token := client.Publish(h.stateTopic, qos, retain, payload)
	if token.Wait() && token.Error() != nil {
		logger.Error().Err(token.Error()).Str("topic", h.stateTopic).Msg("mqtt: home assistant state publish failed")
	}
}

func (h *homeAssistantPublisher) CommandTopic() string {
	if h == nil {
		return ""
	}
	return h.commandTopic
}

func uniqueID(clientID, objectID string) string {
	base := strings.TrimSpace(clientID)
	if base == "" {
		base = "quarc-mqtt"
	}
	return fmt.Sprintf("%s_%s", base, strings.TrimSpace(objectID))
}
