package segmentio

import (
	"os"
	"testing"
	"time"

	maasModel "github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/stretchr/testify/require"
)

func Test_getAvailableData_PLAINTEXT_nocreds(t *testing.T) {
	assertions := require.New(t)
	servers, config, mechanism, err := getAvailableData(&maasModel.TopicConnectionProperties{
		TopicName:        "test",
		NumPartitions:    1,
		Protocol:         "PLAINTEXT",
		BootstrapServers: []string{"test.kafka:9092"},
		SaslMechanism:    "",
		Username:         "",
		Password:         "",
	})
	assertions.NoError(err)
	assertions.Equal([]string{"test.kafka:9092"}, servers)
	assertions.Nil(config)
	assertions.Nil(mechanism)
}

func Test_getAvailableData_PLAINTEXT(t *testing.T) {
	assertions := require.New(t)
	servers, config, mechanism, err := getAvailableData(&maasModel.TopicConnectionProperties{
		TopicName:        "test",
		NumPartitions:    1,
		Protocol:         "PLAINTEXT",
		BootstrapServers: []string{"test.kafka:9092"},
		SaslMechanism:    "PLAIN",
		Username:         "test-username",
		Password:         "test-password",
	})
	assertions.NoError(err)
	assertions.Nil(config)
	assertions.Equal([]string{"test.kafka:9092"}, servers)
	assertions.Equal(plain.Mechanism{Username: "test-username", Password: "test-password"}, mechanism)
}

func Test_getAvailableData_PLAINTEXT_SCRAM(t *testing.T) {
	assertions := require.New(t)
	servers, config, mechanism, err := getAvailableData(&maasModel.TopicConnectionProperties{
		TopicName:        "test",
		NumPartitions:    1,
		Protocol:         "PLAINTEXT",
		BootstrapServers: []string{"test.kafka:9092"},
		SaslMechanism:    "SCRAM-SHA-512",
		Username:         "test-username",
		Password:         "test-password",
	})
	assertions.NoError(err)
	assertions.Nil(config)
	assertions.Equal([]string{"test.kafka:9092"}, servers)
	assertions.Equal("SCRAM-SHA-512", mechanism.Name())
}

func Test_NewDialerAndServers_SASL_PLAINTEXT(t *testing.T) {
	assertions := require.New(t)
	dialer, servers, err := NewDialerAndServers(maasModel.TopicAddress{
		TopicName:       "test",
		NumPartitions:   1,
		BoostrapServers: map[string][]string{"SASL_PLAINTEXT": {"test.kafka:9092"}},
		Credentials: map[string]maasModel.TopicUserCredentials{"SCRAM": {
			Username: "test-username",
			Password: "test-password",
		}},
	})
	assertions.NoError(err)
	assertions.NotNil(dialer)
	assertions.Equal([]string{"test.kafka:9092"}, servers)
	assertions.NotNil(dialer.SASLMechanism)
}

func Test_NewWriterWithOptions(t *testing.T) {
	assertions := require.New(t)
	writer, err := NewWriter(maasModel.TopicAddress{
		TopicName:       "test",
		NumPartitions:   1,
		BoostrapServers: map[string][]string{"SASL_PLAINTEXT": {"test.kafka:9092"}},
		Credentials: map[string]maasModel.TopicUserCredentials{"SCRAM": {
			Username: "test-username",
			Password: "test-password",
		}},
	}, WriterOptions{AlterTransport: func(transport *kafka.Transport) (*kafka.Transport, error) {
		transport.MetadataTTL = time.Minute
		transport.IdleTimeout = 2 * time.Minute
		transport.DialTimeout = 3 * time.Minute
		return transport, nil
	}})
	assertions.NoError(err)
	assertions.NotNil(writer)
	assertions.NotNil(writer.Transport)
	kafkaTransport, ok := writer.Transport.(*kafka.Transport)
	assertions.True(ok)
	assertions.Equal(time.Minute, kafkaTransport.MetadataTTL)
	assertions.Equal(2*time.Minute, kafkaTransport.IdleTimeout)
	assertions.Equal(3*time.Minute, kafkaTransport.DialTimeout)
}

func Test_NewReaderConfigWithOptions(t *testing.T) {
	assertions := require.New(t)
	readerConfig, err := NewReaderConfig(maasModel.TopicAddress{
		TopicName:       "test",
		NumPartitions:   1,
		BoostrapServers: map[string][]string{"SASL_PLAINTEXT": {"test.kafka:9092"}},
		Credentials: map[string]maasModel.TopicUserCredentials{"SCRAM": {
			Username: "test-username",
			Password: "test-password",
		}},
	}, "test-group-id", ReaderOptions{AlterDialer: func(dialer *kafka.Dialer) (*kafka.Dialer, error) {
		dialer.Timeout = time.Minute
		dialer.KeepAlive = 2 * time.Minute
		dialer.FallbackDelay = 3 * time.Minute
		return dialer, nil
	}})
	assertions.NoError(err)
	assertions.NotNil(readerConfig)
	assertions.NotNil(readerConfig.Dialer)
	assertions.Equal(time.Minute, readerConfig.Dialer.Timeout)
	assertions.Equal(2*time.Minute, readerConfig.Dialer.KeepAlive)
	assertions.Equal(3*time.Minute, readerConfig.Dialer.FallbackDelay)
}

func Test_NewClientWithOptions(t *testing.T) {
	assertions := require.New(t)
	client, err := NewClient(maasModel.TopicAddress{
		TopicName:       "test",
		NumPartitions:   1,
		BoostrapServers: map[string][]string{"SASL_PLAINTEXT": {"test.kafka:9092"}},
		Credentials: map[string]maasModel.TopicUserCredentials{"SCRAM": {
			Username: "test-username",
			Password: "test-password",
		}},
	}, ClientOptions{AlterTransport: func(transport *kafka.Transport) (*kafka.Transport, error) {
		transport.MetadataTTL = time.Minute
		transport.IdleTimeout = 2 * time.Minute
		transport.DialTimeout = 3 * time.Minute
		return transport, nil
	}})
	assertions.NoError(err)
	assertions.NotNil(client)
	assertions.NotNil(client.Transport)
	kafkaTransport, ok := client.Transport.(*kafka.Transport)
	assertions.True(ok)
	assertions.Equal(time.Minute, kafkaTransport.MetadataTTL)
	assertions.Equal(2*time.Minute, kafkaTransport.IdleTimeout)
	assertions.Equal(3*time.Minute, kafkaTransport.DialTimeout)
}

func Test_getAvailableData_SASL_SSL_PLAIN(t *testing.T) {
	assertions := require.New(t)
	certBytes, err := os.ReadFile("test/test-cert.pem")
	assertions.NoError(err)
	clientCertBytes, err := os.ReadFile("test/test-client-cert.pem")
	assertions.NoError(err)
	privateKeyBytes, err := os.ReadFile("test/test-private-key.pem")
	assertions.NoError(err)
	servers, config, mechanism, err := getAvailableData(&maasModel.TopicConnectionProperties{
		TopicName:        "test",
		NumPartitions:    1,
		BootstrapServers: []string{"test.kafka:9092"},
		CACert:           string(certBytes),
		Protocol:         "SASL_SSL",
		SaslMechanism:    "PLAIN",
		ClientCert:       string(clientCertBytes),
		ClientKey:        string(privateKeyBytes),
		Username:         "test-username",
		Password:         "test-password",
	})
	assertions.NoError(err)
	assertions.Equal([]string{"test.kafka:9092"}, servers)
	assertions.NotNil(config)
	assertions.NotNil(config.RootCAs)
	assertions.True(len(config.Certificates) > 0)
	expectedMechanism := plain.Mechanism{Username: "test-username", Password: "test-password"}
	assertions.Equal(expectedMechanism, mechanism)
}

func Test_getAvailableData_SASL_SSL_PLAIN_no_client_certs(t *testing.T) {
	assertions := require.New(t)
	certBytes, err := os.ReadFile("test/test-cert.pem")
	assertions.NoError(err)
	servers, config, mechanism, err := getAvailableData(&maasModel.TopicConnectionProperties{
		TopicName:        "test",
		NumPartitions:    1,
		BootstrapServers: []string{"test.kafka:9092"},
		CACert:           string(certBytes),
		Protocol:         "SASL_SSL",
		SaslMechanism:    "PLAIN",
		Username:         "test-username",
		Password:         "test-password",
	})
	assertions.NoError(err)
	assertions.Equal([]string{"test.kafka:9092"}, servers)
	assertions.NotNil(config)
	assertions.NotNil(config.RootCAs)
	expectedMechanism := plain.Mechanism{Username: "test-username", Password: "test-password"}
	assertions.Equal(expectedMechanism, mechanism)
}

func Test_getAvailableData_SASL_SSL_SCRAM(t *testing.T) {
	assertions := require.New(t)
	certBytes, err := os.ReadFile("test/test-cert.pem")
	assertions.NoError(err)
	clientCertBytes, err := os.ReadFile("test/test-client-cert.pem")
	assertions.NoError(err)
	privateKeyBytes, err := os.ReadFile("test/test-private-key.pem")
	assertions.NoError(err)
	servers, config, mechanism, err := getAvailableData(&maasModel.TopicConnectionProperties{
		TopicName:        "test",
		NumPartitions:    1,
		BootstrapServers: []string{"test.kafka:9092"},
		CACert:           string(certBytes),
		Protocol:         "SASL_SSL",
		SaslMechanism:    "SCRAM-SHA-512",
		ClientCert:       string(clientCertBytes),
		ClientKey:        string(privateKeyBytes),
		Username:         "test-username",
		Password:         "test-password",
	})
	assertions.NoError(err)
	assertions.Equal([]string{"test.kafka:9092"}, servers)
	assertions.NotNil(config)
	assertions.NotNil(config.RootCAs)
	assertions.True(len(config.Certificates) > 0)
	assertions.Equal("SCRAM-SHA-512", mechanism.Name())
}

func Test_getAvailableData_SASL_SSL_SCRAM_no_client_certs(t *testing.T) {
	assertions := require.New(t)
	certBytes, err := os.ReadFile("test/test-cert.pem")
	assertions.NoError(err)
	servers, config, mechanism, err := getAvailableData(&maasModel.TopicConnectionProperties{
		TopicName:        "test",
		NumPartitions:    1,
		BootstrapServers: []string{"test.kafka:9092"},
		CACert:           string(certBytes),
		Protocol:         "SASL_SSL",
		SaslMechanism:    "SCRAM-SHA-512",
		Username:         "test-username",
		Password:         "test-password",
	})
	assertions.NoError(err)
	assertions.Equal([]string{"test.kafka:9092"}, servers)
	assertions.NotNil(config)
	assertions.NotNil(config.RootCAs)
	assertions.True(len(config.Certificates) == 0)
	assertions.Equal("SCRAM-SHA-512", mechanism.Name())
}

func Test_getAvailableData_SSL(t *testing.T) {
	assertions := require.New(t)
	certBytes, err := os.ReadFile("test/test-cert.pem")
	assertions.NoError(err)
	servers, config, mechanism, err := getAvailableData(&maasModel.TopicConnectionProperties{
		TopicName:        "test",
		NumPartitions:    1,
		BootstrapServers: []string{"test.kafka:9092"},
		CACert:           string(certBytes),
		Protocol:         "SSL",
	})
	assertions.NoError(err)
	assertions.Equal([]string{"test.kafka:9092"}, servers)
	assertions.NotNil(config)
	assertions.NotNil(config.RootCAs)
	assertions.Nil(mechanism)
}
