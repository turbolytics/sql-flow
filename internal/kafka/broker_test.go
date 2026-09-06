package kafka

// The integration tests provision their own broker.
//
// They used to dial localhost:9092 and skip when nothing answered, which meant
// they ran only where someone had already started the dev stack -- so in
// practice, never in CI. Requiring the stack instead of skipping would have
// fixed the honesty problem and left `go test ./...` failing on a fresh clone
// until the contributor knew which make target to run first. A test that
// starts what it needs has neither problem.

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

// brokerImage is pinned so a broker upgrade is a commit rather than a Tuesday.
// confluent-local is the KRaft image the testcontainers module is built
// around; the dev stack's cp-kafka needs a separate ZooKeeper.
const brokerImage = "confluentinc/confluent-local:7.5.0"

var (
	brokerOnce sync.Once
	brokerAddr string
	brokerErr  error
	brokerCtr  *tckafka.KafkaContainer
)

// brokerOrFail returns a broker address, starting a container on first use.
//
// -short is the only way out, and it is what the unit pass runs. Everywhere
// else a missing broker fails the test rather than skipping it: these six are
// the only thing that exercises commit semantics, offset resume and the high
// watermark, and a skip reads as "ok" in the log and as coverage in the
// matrix. That is how the Iceberg sink shipped untested.
func brokerOrFail(t *testing.T) string {
	t.Helper()

	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}

	// An explicit broker wins, so a dev stack or a CI service can be used
	// instead of paying for a container per test run.
	if addr := os.Getenv("SQLFLOW_KAFKA_BROKERS"); addr != "" {
		return addr
	}

	brokerOnce.Do(startBroker)
	if brokerErr != nil {
		t.Fatalf("could not start a kafka container: %v\n"+
			"the integration pass needs docker, or set SQLFLOW_KAFKA_BROKERS "+
			"to a broker that is already running", brokerErr)
	}
	return brokerAddr
}

// startBroker runs one container for the whole package. Six tests at ten
// seconds each would dominate the pass, and nothing needs the isolation: every
// test derives its topic and group from the clock, so they cannot collide.
func startBroker() {
	ctx := context.Background()

	ctr, err := tckafka.Run(ctx, brokerImage)
	if err != nil {
		brokerErr = err
		return
	}
	brokerCtr = ctr

	brokers, err := ctr.Brokers(ctx)
	if err != nil {
		brokerErr = err
		return
	}
	if len(brokers) == 0 {
		brokerErr = fmt.Errorf("kafka container reported no brokers")
		return
	}
	brokerAddr = brokers[0]
}

// TestMain stops the container the package started. Ryuk would reap it anyway,
// but only after a delay, and a developer running the pass in a loop should
// not accumulate brokers.
func TestMain(m *testing.M) {
	code := m.Run()

	if brokerCtr != nil {
		// Teardown must not change the verdict: the tests have already run.
		if err := brokerCtr.Terminate(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "leaving the kafka container for ryuk: %v\n", err)
		}
	}
	os.Exit(code)
}
