// Package main demonstrates the v1.4.0 async mirror feature.
//
// Helix's mirror feature is designed for seamless Cassandra cluster
// migrations. The application keeps writing to the *current* dual-cluster
// pair while helix asynchronously mirrors selected writes to a *new*
// dual-cluster pair so that history accumulates on the new pair before
// traffic cutover.
//
// Two deployment modes are demonstrated:
//
//  1. Target mode (helix.WithMirror): the mirror destination is a second
//     helix CQLClient running in this process. Best for dev, test, and
//     small deployments.
//
//  2. Publisher mode (helix.WithMirrorPublisher): captures are published
//     to a NATS-backed Replayer; a separate consumer binary drains them
//     into the mirror destination. Best for production migrations.
//
// # Prerequisites
//
// This example uses live gocql sessions and will not run without two
// Cassandra clusters reachable on localhost. The publisher and consumer
// snippets additionally require a NATS server with JetStream enabled.
//
// # Running
//
//	go run main.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/arloliu/helix"
	v1 "github.com/arloliu/helix/adapter/cql/v1"
	"github.com/arloliu/helix/mirror"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
	"github.com/gocql/gocql"
)

func main() {
	fmt.Println("=== Helix Async Mirror Demo ===")
	fmt.Println()
	fmt.Println("Run targetMode() against two reachable Cassandra clusters.")
	fmt.Println("Run publisherApp() and publisherConsumer() against NATS + Cassandra.")
}

// targetMode shows the simplest mirror wiring: a second helix CQLClient is
// the mirror destination, and the engine dispatches each captured write
// directly through it. Optional WithMirrorReplayer adds durable retry of
// failed mirror writes via an in-memory queue (substitute NATSReplayer for
// production durability).
func targetMode() {
	mirrorTarget := buildClient([]string{"new-a:9042"}, []string{"new-b:9042"})
	defer mirrorTarget.Close()

	primary, err := helix.NewCQLClient(
		newSession([]string{"current-a:9042"}),
		newSession([]string{"current-b:9042"}),
		helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
		helix.WithMirror(mirrorTarget,
			mirror.WithQueueSize(8192),
			mirror.WithWorkers(4),
			mirror.WithOnDrop(func(p types.ReplayPayload) {
				log.Printf("mirror queue full, dropped capture: %q", p.Query)
			}),
		),
		helix.WithMirrorReplayer(replay.NewMemoryReplayer(replay.WithQueueCapacity(2048))),
	)
	if err != nil {
		log.Fatalf("primary client: %v", err)
	}
	defer primary.Close()

	session := primary.Session()
	ctx := context.Background()
	if err := session.Query("INSERT INTO t (k, v) VALUES (?, ?)", "k1", "v1").
		Mirror(). // <-- opt this write into mirror
		ExecContext(ctx); err != nil {
		log.Printf("primary write: %v", err)
	}

	if e := primary.Mirror(); e != nil {
		fmt.Printf("mirror enabled=%v stats=%+v\n", e.Enabled(), e.Stats())
	}
}

// publisherApp shows the recommended production wiring on the application
// side: captures are published to a NATS-backed Replayer instead of being
// written in-process. The actual writes against the mirror clusters happen
// in publisherConsumer (typically a separate binary).
//
// js is a jetstream.JetStream handle obtained from your NATS connection.
//
//	import "github.com/nats-io/nats.go"
//	import "github.com/nats-io/nats.go/jetstream"
//
//	nc, _ := nats.Connect("nats://localhost:4222")
//	js, _ := jetstream.New(nc)
//	publisherApp(js)
func publisherApp(natsReplayer helix.Replayer) {
	app, err := helix.NewCQLClient(
		newSession([]string{"current-a:9042"}),
		newSession([]string{"current-b:9042"}),
		helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
		helix.WithMirrorPublisher(natsReplayer,
			mirror.WithQueueSize(8192),
			mirror.WithWorkers(4),
		),
	)
	if err != nil {
		log.Fatalf("app client: %v", err)
	}
	defer app.Close()

	session := app.Session()
	ctx := context.Background()
	if err := session.Query("INSERT INTO t (k, v) VALUES (?, ?)", "k2", "v2").
		Mirror().
		ExecContext(ctx); err != nil {
		log.Printf("app write: %v", err)
	}
}

// publisherConsumer is the consumer-side counterpart to publisherApp. Run
// in a separate binary; it drains the same NATS replayer and applies the
// captures to the mirror clusters via the helix mirror target.
func publisherConsumer(natsReplayer helix.Replayer) {
	mirrorTarget := buildClient([]string{"new-a:9042"}, []string{"new-b:9042"})
	defer mirrorTarget.Close()

	worker, err := helix.NewMirrorWorker(natsReplayer, mirrorTarget,
		replay.WithMaxAttempts(5),
		replay.WithRetryDelay(100*time.Millisecond),
	)
	if err != nil {
		log.Fatalf("mirror worker: %v", err)
	}
	if err := worker.Start(); err != nil {
		log.Fatalf("worker start: %v", err)
	}
	defer worker.Stop()

	fmt.Println("worker started; draining mirror captures from NATS...")
	// In a real consumer, block on signal here.
}

// --- helpers ---

func buildClient(hostsA, hostsB []string) *helix.CQLClient {
	c, err := helix.NewCQLClient(newSession(hostsA), newSession(hostsB),
		helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
	)
	if err != nil {
		log.Fatalf("build client: %v", err)
	}

	return c
}

func newSession(hosts []string) *v1.Session {
	cluster := gocql.NewCluster(hosts...)
	cluster.Timeout = 2 * time.Second
	s, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("session %v: %v", hosts, err)
	}

	return v1.NewSession(s)
}

// silence unused-symbol warnings when the example is compiled but not run
// against live infrastructure.
var (
	_ = targetMode
	_ = publisherApp
	_ = publisherConsumer
)
