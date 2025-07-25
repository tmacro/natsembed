package natsembed

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestStartAndPublishSubscribe(t *testing.T) {
	// Ensure a clean slate
	_ = Stop()

	err := Start(
		ServerName("test-start"),
		DontListen(), // keep it in‑process only
	)
	if err != nil {
		t.Fatalf("start failed: %v", err)
	}
	// Clean up after the test
	t.Cleanup(func() {
		_ = Stop()
	})

	nc, err := InProcessConnection()
	if err != nil {
		t.Fatalf("in-process connection failed: %v", err)
	}
	t.Cleanup(func() { _ = nc.Drain() })

	msgCh := make(chan *nats.Msg, 1)
	_, err = nc.Subscribe("foo", func(m *nats.Msg) { msgCh <- m })
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	_ = nc.Flush()

	if err := nc.Publish("foo", []byte("bar")); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case m := <-msgCh:
		if string(m.Data) != "bar" {
			t.Fatalf("unexpected message: %q", string(m.Data))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestRunAndContextCancel(t *testing.T) {
	// Ensure a clean slate
	_ = Stop()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- Run(ctx,
			ServerName("test-run"),
			DontListen(),
		)
	}()

	// Give it a moment to start
	time.Sleep(200 * time.Millisecond)

	// Server should be running now; connection should succeed
	nc, err := InProcessConnection()
	if err != nil {
		cancel()
		t.Fatalf("in-process connection failed: %v", err)
	}
	_ = nc.Drain()

	// Cancel context, Run should return
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run did not return after context cancel")
	}
}

func TestReconfigureClosesConnectionsAndReconnects(t *testing.T) {
	// Ensure a clean slate
	_ = Stop()

	// Start server
	if err := Start(ServerName("rcfg-1"), DontListen()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	t.Cleanup(func() { _ = Stop() })

	nc, err := InProcessConnection()
	if err != nil {
		t.Fatalf("in-process connection failed: %v", err)
	}
	defer nc.Drain()

	// Subscribe before reconfigure
	msgCh := make(chan *nats.Msg, 1)
	_, err = nc.Subscribe("rcfg", func(m *nats.Msg) { msgCh <- m })
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	_ = nc.Flush()

	// Reconfigure with a new option (JetStream enabled as an example)
	if err := Reconfigure(ServerName("rcfg-2"), DontListen(), JetstreamEnabled()); err != nil {
		t.Fatalf("reconfigure failed: %v", err)
	}

	// Publish after reconfigure – the connection should have reconnected automatically
	if err := nc.Publish("rcfg", []byte("ok")); err != nil {
		t.Fatalf("publish after reconfigure failed: %v", err)
	}

	select {
	case m := <-msgCh:
		if string(m.Data) != "ok" {
			t.Fatalf("unexpected message: %q", string(m.Data))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for message after reconfigure")
	}
}

func TestPeerURL(t *testing.T) {
	p := Peer{Host: "example", Port: 4222}
	if want, got := "nats://example:4222", p.URL(); want != got {
		t.Fatalf("unexpected Peer.URL(): want %s, got %s", want, got)
	}
}
