package mqtt

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/zeebo/assert"
)

// The POC's broker settings minus persistence. The tests restart no broker,
// and max_queued_messages 0 is the one setting that decides whether the
// broker keeps what it holds for an offline session.
const mosquittoConf = "listener 1883\nallow_anonymous true\nmax_queued_messages 0\n"

var (
	brokerOnce sync.Once
	brokerURL  string
	brokerErr  error
	brokerCtr  *testcontainers.DockerContainer
)

// brokerOrFail returns a Mosquitto to test against. SQLFLOW_MQTT_BROKER
// points at one already running. Otherwise one container starts per package.
// Without Docker the test fails: an integration test that skips proves
// nothing.
func brokerOrFail(t *testing.T) *url.URL {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	raw := os.Getenv("SQLFLOW_MQTT_BROKER")
	if raw == "" {
		brokerOnce.Do(startBroker)
		if brokerErr != nil {
			t.Fatalf("starting mosquitto: %v", brokerErr)
		}
		raw = brokerURL
	}
	u, err := url.Parse(raw)
	assert.NoError(t, err)
	return u
}

func startBroker() {
	ctx := context.Background()
	c, err := testcontainers.Run(ctx, "eclipse-mosquitto:2.1.2-alpine",
		testcontainers.WithExposedPorts("1883/tcp"),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader:            strings.NewReader(mosquittoConf),
			ContainerFilePath: "/mosquitto/config/mosquitto.conf",
			FileMode:          0o644,
		}),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("1883/tcp")),
	)
	if err != nil {
		brokerErr = err
		return
	}
	brokerCtr = c
	brokerURL, brokerErr = c.PortEndpoint(ctx, "1883/tcp", "tcp")
}

// TestMain stops the container the package started, so a developer running
// the pass in a loop does not accumulate brokers.
func TestMain(m *testing.M) {
	code := m.Run()
	if brokerCtr != nil {
		if err := brokerCtr.Terminate(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "leaving the mosquitto container for ryuk: %v\n", err)
		}
	}
	os.Exit(code)
}

// publish sends n QoS 1 publishes with payload {"i":<n>} and waits for each
// PUBACK.
func publish(t *testing.T, broker *url.URL, topic string, from, to int, retain bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cm, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
		ServerUrls: []*url.URL{broker},
		KeepAlive:  20,
		ClientConfig: paho.ClientConfig{
			ClientID: fmt.Sprintf("pub-%d", time.Now().UnixNano()),
		},
	})
	assert.NoError(t, err)
	assert.NoError(t, cm.AwaitConnection(ctx))
	defer cm.Disconnect(context.Background())
	for i := from; i < to; i++ {
		_, err := cm.Publish(ctx, &paho.Publish{
			QoS: 1, Retain: retain, Topic: topic,
			Payload: []byte(fmt.Sprintf(`{"i":%d}`, i)),
		})
		assert.NoError(t, err)
	}
}

func uniq(prefix string) string { return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano()) }
