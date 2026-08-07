package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
    "io"
    "log"
    "net"
    "net/http"
    "os"
	"path"
    "sync"
    "time"

    "github.com/hashicorp/raft"
    "github.com/hashicorp/raft-boltdb"
)

// Core objects
type HTTPServer struct {
	raft *raft.Raft
	db   *sync.Map
}

type FSM struct {
	db *sync.Map
}

type ledgerPayload struct {
	Op    string
    Key   string
    Value []byte
}

func (fsm *FSM) Apply(log *raft.Log) any {
    switch log.Type {
    case raft.LogCommand:
        var lp ledgerPayload
        err := json.Unmarshal(log.Data, &lp)
        if err != nil {
            return fmt.Errorf("Could not parse payload: %s", err)
        }
        fsm.db.Store(lp.Key, lp.Value)
    default:
        return fmt.Errorf("Unknown raft log type: %#v", log.Type)
    }

    return nil
}

// Naive snapshotting to begin
type snapshot struct{ db *sync.Map }
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) { return &snapshot{db: f.db}, nil }
func (f *FSM) Restore(rc io.ReadCloser) error      { return nil }
func (s *snapshot) Persist(sink raft.SnapshotSink) error { 
	sink.Write([]byte(`{}`)); return sink.Close() 
}
func (s *snapshot) Release() {}


// simple health check
func (h *HTTPServer) healthzHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// Raft request-to-join handler for leader
func (h *HTTPServer) joinHandler(w http.ResponseWriter, r *http.Request) {
	if h.raft.State() != raft.Leader {
		http.Error(w, "Not the cluster leader", http.StatusMethodNotAllowed)
		return
	}

	queryParams := r.URL.Query()
	id := queryParams.Get("id")
	addr := queryParams.Get("addr")

	if id == "" || addr == "" {
		http.Error(w, "Missing id or addr query params", http.StatusBadRequest)
		return
	}

	fut := h.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
	if err := fut.Error(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ------------
// Step Handler
// ------------
type StepRequest struct {
	StepID  string `json:"step_id"`
	Session string `json:"session"`
	Kind    string `json:"kind"`
	Spec    []byte `json:"spec"`
}

type StepResponse struct {
	Result json.RawMessage `json:"result,omitempty"`
	Error  json.RawMessage `json:"error,omitempty"`
}

func (h *HTTPServer) stepHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	
	// Parse body
	var sr StepRequest
	var response StepResponse
	err := json.NewDecoder(r.Body).Decode(&sr)
	if err != nil {
		response.Error = json.RawMessage("{\"error\": \"Invalid JSON\"}")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(response) 
		return
	}

	// Put SubmitStep{step_id} on ledger
	c := ledgerPayload{Op: "SubmitStep", Key: sr.StepID, Value: sr.Spec}
	op, _ := json.Marshal(c)
	fut := h.raft.Apply(op, 1*time.Second)

	if err := fut.Error(); err != nil {
		response.Error = json.RawMessage("{\"error\": \"Apply failed\"}")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(response) 
		return
	}

	// Wait for CommitResult{step_id} from ledger


	// Return to UI
	response.Result = json.RawMessage("{\"result\": \"something\"}")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response) 
}

// -------------
// Hash Handlers
// -------------
func (h *HTTPServer) hashGetHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/octet-stream")
	hashID := r.PathValue("hash")

	val, found := h.db.Load(hashID)
	if !found {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if b, ok := val.([]byte); ok {
    	w.Write(b)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (h *HTTPServer) hashSetHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	hashID := r.PathValue("hash")
	bodyBytes, _ := io.ReadAll(r.Body)
	bodyHash := sha256.Sum256(bodyBytes)

	// Verify if they match
	if hex.EncodeToString(bodyHash[:]) != hashID {
		fmt.Println("Mismatch! Hash is invalid.")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// Verify idempotence
	_, found := h.db.Load(hashID)
	if found {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Apply to FSM
	c := ledgerPayload{Op: "PutBlob", Key: hashID, Value: bodyBytes}
	op, _ := json.Marshal(c)
	fut := h.raft.Apply(op, 1*time.Second)
	if err := fut.Error(); err != nil {
		http.Error(w, fmt.Sprintf("Failed to write value: %s", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

//func (h *HTTPServer) hashMissingHandler(w http.ResponseWriter, r *http.Request) {
//}

// ------------------
// Service Entrypoint
// ------------------
func main() {

	// Setup command-line options
	nodeID := flag.String("id", "node1", "Unique node identifier")
	httpAddr := flag.String("http", "127.0.0.1:4700", "HTTP server address")
	// execAddr := flag.String("executor", "127.0.0.1:4701", "Local executor address")
	raftAddr := flag.String("raft", "127.0.0.1:7000", "Raft comms address")
	joinAddr := flag.String("join", "", "HTTP address of the existing leader to join (leave blank for node1)")
	flag.Parse()

	// Make service objects
	db := &sync.Map{}
	fsm := &FSM{db: db}

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
	srv := &HTTPServer{raft: r, db: db}
	http.HandleFunc("GET /join", srv.joinHandler)
	http.HandleFunc("GET /healthz", srv.healthzHandler)
	
	http.HandleFunc("POST /v1/step", srv.stepHandler)

	// Blob storage interface
	http.HandleFunc("GET /v1/blob/{hash}", srv.hashGetHandler)
	http.HandleFunc("PUT /v1/blob/{hash}", srv.hashSetHandler)
	// http.HandleFunc("POST /v1/blob/missing", srv.hashMissingHandler)

	// Automatically attempt connection if a join target was passed
	if *joinAddr != "" {
		go func() {
			// Small pause to allow local Raft transport initialization
			time.Sleep(1 * time.Second)
			joinURL := fmt.Sprintf("http://%s/join?id=%s&addr=%s", *joinAddr, *nodeID, *raftAddr)
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
