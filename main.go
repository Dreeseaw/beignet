package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// Core objects
type HTTPServer struct {
	raft     *raft.Raft
	blobs    *sync.Map
	steps    *sync.Map
	nodes    *sync.Map
	execErr  *sync.Map // node-local: stepID -> last executor failure (not replicated)
	nodeID   string
	httpAddr string
	execAddr string
}

func main() {

	// Setup command-line options
	nodeID := flag.String("id", "node1", "Unique node identifier")
	httpAddr := flag.String("http", "127.0.0.1:4700", "HTTP server address")
	execAddr := flag.String("executor", "http://127.0.0.1:4701", "Local executor base URL")
	raftAddr := flag.String("raft", "127.0.0.1:7000", "Raft comms address")
	joinAddr := flag.String("join", "", "HTTP address of the existing leader to join (leave blank for node1)")
	flag.Parse()

	// Make service objects
	blobs := &sync.Map{}
	steps := &sync.Map{}
	nodes := &sync.Map{}
	fsm := &FSM{blobs: blobs, steps: steps, nodes: nodes}

	// Raft Configurations
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(*nodeID)

	// Create directory for log files
	baseDir := path.Join("raft-data", *nodeID)
	os.MkdirAll(baseDir, 0755)

	// Setup BoltDB Storage (default for hashicorp/raft implementations)
	logStore, err := raftboltdb.NewBoltStore(path.Join(baseDir, "raft-log.db"))
	if err != nil {
		log.Fatalf("failed to init log store: %v", err)
	}
	stableStore, err := raftboltdb.NewBoltStore(path.Join(baseDir, "raft-stable.db"))
	if err != nil {
		log.Fatalf("failed to init stable store: %v", err)
	}
	snapshotStore, err := raft.NewFileSnapshotStore(baseDir, 3, os.Stderr)
	if err != nil {
		log.Fatalf("failed to init snapshot store: %v", err)
	}

	// Establish network layer for inter-node communication
	tcpAddr, err := net.ResolveTCPAddr("tcp", *raftAddr)
	if err != nil {
		log.Fatalf("Failed to resolve TCP address: %v", err)
	}
	transport, err := raft.NewTCPTransport(*raftAddr, tcpAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		log.Fatalf("Failed to create TCP transport: %v", err)
	}

	// Construct Raft instance
	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		log.Fatalf("Failed to construct Raft instance: %v", err)
	}

	// Bootstrap cluster if it's the first node and no join target was provided
	if *joinAddr == "" {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(configuration)
		log.Println("Bootstrapping single-node cluster...")
	}

	// Start HTTP Server
	srv := &HTTPServer{
		raft:     r,
		blobs:    blobs,
		steps:    steps,
		nodes:    nodes,
		execErr:  &sync.Map{},
		nodeID:   *nodeID,
		httpAddr: *httpAddr,
		execAddr: *execAddr,
	}
	http.HandleFunc("GET /join", srv.joinHandler)
	http.HandleFunc("GET /healthz", srv.healthzHandler)
	http.HandleFunc("GET /readyz", srv.readyzHandler)
	http.HandleFunc("GET /v1/status", srv.statusHandler)

	http.HandleFunc("POST /v1/step", srv.stepHandler)

	// Blob storage interface
	http.HandleFunc("GET /v1/blob/{hash}", srv.hashGetHandler)
	http.HandleFunc("PUT /v1/blob/{hash}", srv.hashSetHandler)
	http.HandleFunc("POST /v1/blobs/missing", srv.hashMissingHandler)

	// Session watch interface
	http.HandleFunc("GET /v1/session/{session}/steps", srv.sessionStepsHandler)

	// Node-to-node: followers forward writes here
	http.HandleFunc("POST /v1/internal/apply", srv.internalApplyHandler)

	// Claim + execute loop: sweeps the replicated steps map for pending work.
	go srv.claimLoop()
	go srv.tickLoop()
	go srv.registerSelf()

	// Automatically attempt connection if a join target was passed
	if *joinAddr != "" {
		go func() {
			// Small pause to allow local Raft transport initialization
			time.Sleep(1 * time.Second)
			joinURL := fmt.Sprintf("http://%s/join?id=%s&addr=%s&http=%s",
				*joinAddr, *nodeID, *raftAddr, *httpAddr)
			log.Printf("Attempting to join cluster via: %s", joinURL)
			resp, err := http.Get(joinURL)
			if err != nil || resp.StatusCode != http.StatusOK {
				log.Printf("Join cluster handshake failed: %v", err)
			}
		}()
	}

	log.Printf("Starting HTTP API service on %s", *httpAddr)
	if err := http.ListenAndServe(*httpAddr, nil); err != nil {
		log.Fatalf("HTTP server shutdown error: %v", err)
	}
}
