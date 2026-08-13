package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func stringReader(s string) io.Reader { return strings.NewReader(s) }

// A write sent to a FOLLOWER must succeed: it proxies to the leader.
// Before leader forwarding this returned ErrNotLeader.
func TestFollowerAcceptsWrites(t *testing.T) {
	nodes := startCluster(t)
	leader := findLeader(t, nodes)

	var follower *node
	for _, n := range nodes {
		if n.id != leader.id {
			follower = n
			break
		}
	}
	t.Logf("leader=%s follower=%s", leader.id, follower.id)

	payload := `{"written":"via-follower"}`
	status, hash := putBlob(follower.httpAddr, payload)
	if status != http.StatusOK {
		t.Fatalf("PUT to follower %s: HTTP %d (forwarding broken)", follower.id, status)
	}

	for _, n := range nodes {
		n := n
		waitFor(t, n.id+" has the follower-written blob", 10*time.Second, func() bool {
			s, body := getBlob(n.httpAddr, hash)
			return s == http.StatusOK && body == payload
		})
	}
}

// Every node must publish its HTTP address into the ledger, or forwarding
// has nowhere to send.
func TestAllNodesRegisterHTTPAddresses(t *testing.T) {
	nodes := startCluster(t)

	for _, n := range nodes {
		n := n
		waitFor(t, "cluster knows all 3 HTTP addresses via "+n.id, 20*time.Second, func() bool {
			// Submitting a step to each node only works if that node can reach
			// the leader, which requires the address map to be populated.
			body := fmt.Sprintf(`{"step_id":"reg-%s","session":"reg","kind":"tool","spec":{"tool":"bash"}}`, n.id)
			resp, err := http.Post("http://"+n.httpAddr+"/v1/step?wait=false",
				"application/json", stringReader(body))
			if err != nil {
				return false
			}
			resp.Body.Close()
			return resp.StatusCode == http.StatusAccepted
		})
	}
}
