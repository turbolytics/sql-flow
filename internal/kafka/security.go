package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"strings"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// SecurityOptions translates the security_protocol / ssl / sasl config blocks
// into franz-go client options. The config keys mirror librdkafka's, which is
// what the Python engine configures.
func SecurityOptions(protocol string, sslConf *config.KafkaSSL, saslConf *config.KafkaSASL) ([]kgo.Opt, error) {
	var opts []kgo.Opt

	protocol = strings.ToUpper(strings.TrimSpace(protocol))
	switch protocol {
	case "", "PLAINTEXT":
		// Nothing to configure, but SASL_PLAINTEXT below still needs the
		// mechanism, so fall through on an explicit sasl block only.
		if protocol == "" && saslConf == nil {
			return nil, nil
		}
	case "SSL", "SASL_SSL", "SASL_PLAINTEXT":
	default:
		return nil, errs.New(errs.CodeSourceSecurityInvalid, "unsupported security_protocol: %q", protocol)
	}

	if protocol == "SSL" || protocol == "SASL_SSL" {
		tlsConfig, err := tlsConfig(sslConf)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	if protocol == "SASL_SSL" || protocol == "SASL_PLAINTEXT" || saslConf != nil {
		if saslConf == nil {
			return nil, errs.New(errs.CodeSourceSecurityInvalid, "security_protocol %q requires a sasl block", protocol)
		}
		mech, err := saslMechanism(saslConf)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mech))
	}

	return opts, nil
}

func saslMechanism(conf *config.KafkaSASL) (sasl.Mechanism, error) {
	auth := plain.Auth{User: conf.Username, Pass: conf.Password}

	switch strings.ToUpper(strings.TrimSpace(conf.Mechanism)) {
	case "PLAIN":
		return auth.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: conf.Username, Pass: conf.Password}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: conf.Username, Pass: conf.Password}.AsSha512Mechanism(), nil
	default:
		// GSSAPI is in the config schema but needs Kerberos infrastructure
		// franz-go does not provide out of the box.
		return nil, errs.New(errs.CodeSourceSecurityInvalid, "unsupported sasl mechanism: %q", conf.Mechanism)
	}
}

func tlsConfig(conf *config.KafkaSSL) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if conf == nil {
		return cfg, nil
	}

	if conf.CALocation != "" {
		pemBytes, err := os.ReadFile(conf.CALocation)
		if err != nil {
			return nil, errs.Wrap(errs.CodeSourceSecurityInvalid, err, "reading ca_location")
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, errs.New(errs.CodeSourceSecurityInvalid, "ca_location %q contains no usable certificates", conf.CALocation)
		}
		cfg.RootCAs = pool
	}

	if conf.CertificateLocation != "" || conf.KeyLocation != "" {
		if conf.CertificateLocation == "" || conf.KeyLocation == "" {
			return nil, errs.New(errs.CodeSourceSecurityInvalid, "ssl requires both certificate_location and key_location")
		}
		cert, err := loadKeyPair(conf.CertificateLocation, conf.KeyLocation, conf.KeyPassword)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	// librdkafka disables hostname verification with the value "none".
	if strings.EqualFold(strings.TrimSpace(conf.EndpointIdentificationAlgorithm), "none") {
		cfg.InsecureSkipVerify = true
	}

	return cfg, nil
}

func loadKeyPair(certPath, keyPath, keyPassword string) (tls.Certificate, error) {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return tls.Certificate{}, errs.Wrap(errs.CodeSourceSecurityInvalid, err, "reading certificate_location")
	}
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return tls.Certificate{}, errs.Wrap(errs.CodeSourceSecurityInvalid, err, "reading key_location")
	}

	if keyPassword != "" {
		decrypted, err := decryptKeyPEM(keyPEM, keyPassword)
		if err != nil {
			return tls.Certificate{}, err
		}
		keyPEM = decrypted
	}

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, errs.Wrap(errs.CodeSourceSecurityInvalid, err, "loading key pair")
	}
	return cert, nil
}

// decryptKeyPEM handles the legacy encrypted-PEM form librdkafka accepts via
// ssl.key.password. Go removed x509.DecryptPEMBlock as insecure, so this
// reports a clear error rather than pretending to support it.
func decryptKeyPEM(keyPEM []byte, password string) ([]byte, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, errs.New(errs.CodeSourceSecurityInvalid, "key_location does not contain a PEM block")
	}
	//lint:ignore SA1019 detection only; the key is not decrypted here
	if _, encrypted := block.Headers["DEK-Info"]; !encrypted {
		// Not an encrypted PEM: the password is redundant, so use the key.
		return keyPEM, nil
	}
	return nil, errs.New(errs.CodeSourceSecurityInvalid,
		"key_location is an encrypted PEM, which Go's TLS stack cannot read; "+
			"decrypt the key (openssl pkcs8 -topk8 -nocrypt) and drop key_password")
}
