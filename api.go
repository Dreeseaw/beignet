// api.go
// handlers, req/resp structs, helpers

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
	c := Payload{Op: OpType.OpSubmitStep, Key: sr.StepID, Value: sr.Spec}
	op, _ := json.Marshal(c)
	fut := h.raft.Apply(op, 1*time.Second)

	if err := fut.Error(); err != nil {
		response.Error = json.RawMessage("{\"error\": \"Apply failed\"}")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(response) 
		return
	}

	// Wait for CommitResult{step_id} from ledger
	// if ?wait=true then blob for response, else just return a session_id or something

	// Return to UI
	response.Result = json.RawMessage("{\"result\": \"something\"}")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response) 
}

// -------------
// Hash handlers
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
	_, found := h.blobs.Load(hashID)
	if found {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Apply to FSM
	c := Payload{Op: OpType.OpPutBlob, Key: hashID, Value: bodyBytes}
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

// -------------
// Core handlers
// -------------

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
