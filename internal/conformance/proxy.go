package conformance

import (
	"context"
	"testing"

	toxiclient "github.com/Shopify/toxiproxy/v2/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/toxiproxy"
	"github.com/testcontainers/testcontainers-go/network"
)

// toxiproxyImage is pinned so a proxy upgrade is a commit rather than a
// Tuesday.
const toxiproxyImage = "ghcr.io/shopify/toxiproxy:2.12.0"

const (
	// proxyName is the single proxy each container carries.
	proxyName = "upstream"
	// proxyPort is the port the module assigns the first proxy.
	proxyPort = 8666
	// toxicName is the hang Break installs and Heal removes.
	toxicName = "hang"
)

// Proxy is a toxiproxy container between a sink and its destination.
//
// Break adds a timeout toxic, which holds every connection open and never
// answers. That is a partition rather than a refusal, and the distinction
// matters: a refused connection returns immediately, while a hang is what
// #219 fixed the Kafka sink for and what sink.flush.honours_context will
// need. A sink that ignores its context hangs here rather than passing
// quietly.
type Proxy struct {
	// Addr is the host:port a sink dials instead of the destination.
	Addr string

	client *toxiclient.Client
}

// NewProxy starts toxiproxy on nw, forwarding to upstream.
//
// upstream is "<alias>:<port>" of a container already on nw, not a host port:
// the proxy dials it from inside the docker network. The container is
// terminated when the test ends.
func NewProxy(t *testing.T, nw *testcontainers.DockerNetwork, upstream string) *Proxy {
	t.Helper()
	ctx := context.Background()

	ctr, err := toxiproxy.Run(ctx, toxiproxyImage,
		network.WithNetwork([]string{"toxiproxy"}, nw),
		toxiproxy.WithProxy(proxyName, upstream),
	)
	if err != nil {
		t.Fatalf("conformance: start toxiproxy: %v", err)
	}
	t.Cleanup(func() {
		// Teardown must not change the verdict: the test has already run.
		_ = ctr.Terminate(context.Background())
	})

	host, port, err := ctr.ProxiedEndpoint(proxyPort)
	if err != nil {
		t.Fatalf("conformance: toxiproxy proxied endpoint: %v", err)
	}
	uri, err := ctr.URI(ctx)
	if err != nil {
		t.Fatalf("conformance: toxiproxy control uri: %v", err)
	}

	return &Proxy{Addr: host + ":" + port, client: toxiclient.NewClient(uri)}
}

// Break makes every connection through the proxy hang without answering.
func (p *Proxy) Break(t *testing.T) {
	t.Helper()

	proxy, err := p.client.Proxy(proxyName)
	if err != nil {
		t.Fatalf("conformance: look up proxy: %v", err)
	}
	// timeout=0 holds the connection open indefinitely. A positive value
	// closes it after that many milliseconds, which is a refusal in slow
	// motion and a different fault.
	if _, err := proxy.AddToxic(toxicName, "timeout", "downstream", 1.0,
		toxiclient.Attributes{"timeout": 0}); err != nil {
		t.Fatalf("conformance: add the timeout toxic: %v", err)
	}
}

// Heal removes the hang.
func (p *Proxy) Heal(t *testing.T) {
	t.Helper()

	proxy, err := p.client.Proxy(proxyName)
	if err != nil {
		t.Fatalf("conformance: look up proxy: %v", err)
	}
	if err := proxy.RemoveToxic(toxicName); err != nil {
		t.Fatalf("conformance: remove the timeout toxic: %v", err)
	}
}
