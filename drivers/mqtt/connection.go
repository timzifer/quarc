package mqtt

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
)

// buildClient constructs a configured MQTT client and establishes the initial connection.
func buildClient(settings ConnectionSettings, logger zerolog.Logger, onConnect mqtt.OnConnectHandler) (mqtt.Client, error) {
	if settings.Broker == "" {
		return nil, fmt.Errorf("mqtt: broker address is required")
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(settings.Broker)
	if settings.ClientID != "" {
		opts.SetClientID(settings.ClientID)
	}
	if settings.CleanSession != nil {
		opts.SetCleanSession(*settings.CleanSession)
	}
	if settings.Auth != nil {
		opts.SetUsername(settings.Auth.Username)
		opts.SetPassword(settings.Auth.Password)
	}
	if settings.KeepAlive != nil {
		opts.SetKeepAlive(settings.KeepAlive.Duration)
	}
	if settings.ConnectTimeout != nil {
		opts.SetConnectTimeout(settings.ConnectTimeout.Duration)
	}
	if settings.AutoReconnect != nil {
		opts.AutoReconnect = *settings.AutoReconnect
	}
	if settings.MaxReconnect != nil {
		opts.SetMaxReconnectInterval(settings.MaxReconnect.Duration)
	}
	if settings.SessionExpiry != nil {
		opts.SetConnectRetryInterval(settings.SessionExpiry.Duration)
	}

	if settings.TLS != nil && settings.TLS.Enabled {
		tlsConfig, err := buildTLSConfig(*settings.TLS)
		if err != nil {
			return nil, err
		}
		opts.SetTLSConfig(tlsConfig)
	}

	if settings.Will != nil && settings.Will.Topic != "" {
		payload := append([]byte(nil), settings.Will.Payload...)
		if settings.Will.Format != nil {
			var value any
			if len(settings.Will.Payload) > 0 {
				if err := json.Unmarshal(settings.Will.Payload, &value); err != nil {
					value = string(settings.Will.Payload)
				}
			}
			encoded, err := EncodePayload(*settings.Will.Format, value)
			if err != nil {
				return nil, fmt.Errorf("mqtt: encode will payload: %w", err)
			}
			payload = encoded
		}
		qos := byte(0)
		if settings.Will.QoS != nil {
			qos = *settings.Will.QoS
		}
		retain := false
		if settings.Will.Retain != nil {
			retain = *settings.Will.Retain
		}
		opts.SetBinaryWill(settings.Will.Topic, payload, qos, retain)
	}

	if onConnect != nil {
		opts.OnConnect = onConnect
	}

	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		logger.Warn().Err(err).Msg("mqtt: connection lost")
	})
	opts.SetReconnectingHandler(func(_ mqtt.Client, _ *mqtt.ClientOptions) {
		logger.Info().Msg("mqtt: reconnecting")
	})

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(30 * time.Second) {
		return nil, fmt.Errorf("mqtt: connect timeout")
	}
	if err := token.Error(); err != nil {
		return nil, fmt.Errorf("mqtt: connect failed: %w", err)
	}

	return client, nil
}

func buildTLSConfig(settings TLSSettings) (*tls.Config, error) {
	cfg := &tls.Config{InsecureSkipVerify: settings.InsecureSkipVerify}
	if settings.ServerName != "" {
		cfg.ServerName = settings.ServerName
	}
	if len(settings.ALPN) > 0 {
		cfg.NextProtos = append([]string(nil), settings.ALPN...)
	}

	if settings.CAFile != "" {
		ca, err := os.ReadFile(settings.CAFile)
		if err != nil {
			return nil, fmt.Errorf("mqtt: read ca file: %w", err)
		}
		pool := x509.NewCertPool()
		if ok := pool.AppendCertsFromPEM(ca); !ok {
			return nil, fmt.Errorf("mqtt: parse ca file %s", settings.CAFile)
		}
		cfg.RootCAs = pool
	}

	if settings.CertFile != "" && settings.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(settings.CertFile, settings.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("mqtt: load client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}
