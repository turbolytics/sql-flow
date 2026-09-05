package kafka

import (
	"testing"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/zeebo/assert"
)

func TestSourceKafka_SecurityOptionsPlaintextNeedsNoOptions(t *testing.T) {
	opts, err := SecurityOptions("", nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(opts))

	opts, err = SecurityOptions("PLAINTEXT", nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(opts))
}

func TestSourceKafka_SecurityOptionsSASLMechanisms(t *testing.T) {
	for _, mech := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "scram-sha-512"} {
		t.Run(mech, func(t *testing.T) {
			opts, err := SecurityOptions("SASL_PLAINTEXT", nil, &config.KafkaSASL{
				Mechanism: mech, Username: "u", Password: "p",
			})
			assert.NoError(t, err)
			assert.Equal(t, 1, len(opts))
		})
	}
}

func TestSourceKafka_SecurityOptionsRejectsUnknownMechanism(t *testing.T) {
	_, err := SecurityOptions("SASL_PLAINTEXT", nil, &config.KafkaSASL{
		Mechanism: "GSSAPI", Username: "u", Password: "p",
	})
	assert.Error(t, err)
}

func TestSourceKafka_SecurityOptionsRejectsUnknownProtocol(t *testing.T) {
	_, err := SecurityOptions("SASL_MAGIC", nil, nil)
	assert.Error(t, err)
}

func TestSourceKafka_SecurityOptionsSASLProtocolRequiresSASLBlock(t *testing.T) {
	_, err := SecurityOptions("SASL_SSL", nil, nil)
	assert.Error(t, err)
}

func TestSourceKafka_SecurityOptionsSSLAddsDialer(t *testing.T) {
	opts, err := SecurityOptions("SSL", nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(opts))
}

func TestSourceKafka_SecurityOptionsSASLSSLAddsBoth(t *testing.T) {
	opts, err := SecurityOptions("SASL_SSL", nil, &config.KafkaSASL{
		Mechanism: "PLAIN", Username: "u", Password: "p",
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(opts))
}

func TestTLSConfig_DisablesHostnameVerification(t *testing.T) {
	cfg, err := tlsConfig(&config.KafkaSSL{EndpointIdentificationAlgorithm: "none"})
	assert.NoError(t, err)
	assert.That(t, cfg.InsecureSkipVerify)

	cfg, err = tlsConfig(&config.KafkaSSL{})
	assert.NoError(t, err)
	assert.That(t, !cfg.InsecureSkipVerify)
}

func TestTLSConfig_ReportsMissingCAFile(t *testing.T) {
	_, err := tlsConfig(&config.KafkaSSL{CALocation: "/nonexistent/ca.pem"})
	assert.Error(t, err)
}

func TestTLSConfig_RequiresBothCertAndKey(t *testing.T) {
	_, err := tlsConfig(&config.KafkaSSL{CertificateLocation: "/tmp/cert.pem"})
	assert.Error(t, err)
}
