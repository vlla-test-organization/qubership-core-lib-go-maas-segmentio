package segmentio

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	maasKafka "github.com/vlla-test-organization/qubership-core-lib-go-maas-client/v3/kafka"
	maasModel "github.com/vlla-test-organization/qubership-core-lib-go-maas-client/v3/kafka/model"
)

func NewWriter(topic maasModel.TopicAddress, options ...WriterOptions) (*kafka.Writer, error) {
	connectionProps, err := maasKafka.Extract(topic)
	if err != nil {
		return nil, err
	}
	servers, tlsConfig, saslMechanism, err := getAvailableData(connectionProps)
	if err != nil {
		return nil, err
	}
	transport := &kafka.Transport{
		TLS:  tlsConfig,
		SASL: saslMechanism,
	}
	for _, opt := range options {
		if opt.AlterTransport != nil {
			if altered, aErr := opt.AlterTransport(transport); aErr != nil {
				return nil, aErr
			} else {
				transport = altered
			}
		}
	}
	writer := &kafka.Writer{
		Addr:      kafka.TCP(servers...),
		Transport: transport,
		Topic:     topic.TopicName,
	}
	return writer, nil
}

func NewReaderConfig(topic maasModel.TopicAddress, groupId string, options ...ReaderOptions) (*kafka.ReaderConfig, error) {
	dialer, servers, err := NewDialerAndServers(topic)
	if err != nil {
		return nil, err
	}
	for _, opt := range options {
		if opt.AlterDialer != nil {
			if altered, aErr := opt.AlterDialer(dialer); aErr != nil {
				return nil, aErr
			} else {
				dialer = altered
			}
		}
	}
	return &kafka.ReaderConfig{
		Dialer:  dialer,
		Brokers: servers,
		Topic:   topic.TopicName,
		GroupID: groupId,
	}, nil
}

func NewDialerAndServers(topic maasModel.TopicAddress) (*kafka.Dialer, []string, error) {
	connectionProps, err := maasKafka.Extract(topic)
	if err != nil {
		return nil, nil, err
	}
	servers, tlsConfig, saslMechanism, err := getAvailableData(connectionProps)
	if err != nil {
		return nil, nil, err
	}
	dialer := &kafka.Dialer{
		TLS:           tlsConfig,
		SASLMechanism: saslMechanism,
	}
	return dialer, servers, nil
}

func NewClient(topic maasModel.TopicAddress, options ...ClientOptions) (*kafka.Client, error) {
	connectionProps, err := maasKafka.Extract(topic)
	if err != nil {
		return nil, err
	}
	servers, tlsConfig, saslMechanism, err := getAvailableData(connectionProps)
	if err != nil {
		return nil, err
	}
	transport := &kafka.Transport{
		TLS:  tlsConfig,
		SASL: saslMechanism,
	}
	for _, opt := range options {
		if opt.AlterTransport != nil {
			if altered, aErr := opt.AlterTransport(transport); aErr != nil {
				return nil, aErr
			} else {
				transport = altered
			}
		}
	}
	return &kafka.Client{
		Addr:      kafka.TCP(servers...),
		Transport: transport,
	}, nil
}

func getAvailableData(props *maasModel.TopicConnectionProperties) ([]string, *tls.Config, sasl.Mechanism, error) {
	protocol := props.Protocol
	servers := props.BootstrapServers
	switch protocol {
	case "PLAINTEXT":
		mechanism := props.SaslMechanism
		switch mechanism {
		case "":
			return servers, nil, nil, nil
		case "PLAIN":
			m := plain.Mechanism{Username: props.Username, Password: props.Password}
			return servers, nil, m, nil
		case "SCRAM-SHA-512":
			m, err := scram.Mechanism(scram.SHA512, props.Username, props.Password)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("failed to build scram mechanism, cause: %w", err)
			}
			return servers, nil, m, err
		default:
			return nil, nil, nil, fmt.Errorf("unsupported mechanism: %s for protocol: %s ", mechanism, protocol)
		}
	case "SASL_PLAINTEXT":
		mechanism := props.SaslMechanism
		switch mechanism {
		case "SCRAM-SHA-512":
			m, err := scram.Mechanism(scram.SHA512, props.Username, props.Password)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("failed to build scram mechanism, cause: %w", err)
			}
			return servers, nil, m, err
		default:
			return nil, nil, nil, fmt.Errorf("unsupported mechanism: %s for protocol: %s ", mechanism, protocol)
		}
	case "SASL_SSL":
		caCertPool := x509.NewCertPool()
		caCert := props.CACert
		if ok := caCertPool.AppendCertsFromPEM([]byte(caCert)); !ok {
			return nil, nil, nil, errors.New("failed to append ca cert")
		}
		var m sasl.Mechanism
		var err error
		if props.SaslMechanism == "PLAIN" {
			m = plain.Mechanism{Username: props.Username, Password: props.Password}
		} else if props.SaslMechanism == "SCRAM-SHA-512" {
			m, err = scram.Mechanism(scram.SHA512, props.Username, props.Password)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("failed to build scram mechanism, cause: %w", err)
			}
		} else {
			return nil, nil, nil, fmt.Errorf("unsupported SaslMechanism: '%s' for protocol: '%s'", props.SaslMechanism, props.Protocol)
		}
		if props.ClientCert != "" && props.ClientKey != "" {
			clientCertificate, err := tls.X509KeyPair([]byte(props.ClientCert), []byte(props.ClientKey))
			if err != nil {
				return nil, nil, nil, fmt.Errorf("failed to create X509KeyPair, cause: %w", err)
			}
			return servers, &tls.Config{
				Certificates:       []tls.Certificate{clientCertificate},
				RootCAs:            caCertPool,
				InsecureSkipVerify: true,
			}, m, nil
		} else {
			return servers, &tls.Config{RootCAs: caCertPool, InsecureSkipVerify: true}, m, nil
		}
	case "SSL":
		caCert := props.CACert
		caCertPool := x509.NewCertPool()
		if ok := caCertPool.AppendCertsFromPEM([]byte(caCert)); !ok {
			return nil, nil, nil, errors.New("failed to append ca cert")
		}

		tlsConfig := &tls.Config{
			RootCAs:            caCertPool,
			InsecureSkipVerify: true,
		}

		if props.ClientCert != "" && props.ClientKey != "" {
			clientCertificate, err := tls.X509KeyPair([]byte(props.ClientCert), []byte(props.ClientKey))
			if err != nil {
				return nil, nil, nil, fmt.Errorf("failed to create X509KeyPair, cause: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{clientCertificate}
		}

		return servers, tlsConfig, nil, nil
	default:
		return nil, nil, nil, fmt.Errorf("unsupported protocol: %s", protocol)
	}
}

type WriterOptions struct {
	AlterTransport func(transport *kafka.Transport) (*kafka.Transport, error)
}

type ReaderOptions struct {
	AlterDialer func(dialer *kafka.Dialer) (*kafka.Dialer, error)
}

type ClientOptions struct {
	AlterTransport func(transport *kafka.Transport) (*kafka.Transport, error)
}
