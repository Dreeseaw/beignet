package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

var binPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "beignet-bin-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	binPath = filepath.Join(dir, "beignet")
	if out, err := exec.Command("go", "build", "-o", binPath, ".").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build failed: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// ---------- cluster harness ----------

type node struct {
	id       string
	httpAddr string
	raftAddr string
	dir      string
	logPath  string
	cmd      *exec.Cmd
}

func freeAddrs(t *testing.T, count int) []string {
	t.Helper()
	listeners := make([]net.Listener, count)
	for i := range listeners {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = l
	}
	addrs := make([]string, count)
	for i, l := range listeners {
		addrs[i] = l.Addr().String()
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return addrs
}

func newNode(t *testing.T, id, httpAddr, raftAddr string) *node {
	dir := filepath.Join(t.TempDir(), id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return &node{
		id:       id,
		httpAddr: httpAddr,
		raftAddr: raftAddr,
		dir:      dir, // node writes raft-data/<id> relative to cwd
		logPath:  filepath.Join(dir, "node.log"),
	}
}

func (n *node) start(t *testing.T, joinAddr string) {
	t.Helper()
	args := []string{"--id", n.id, "--http", n.httpAddr, "--raft", n.raftAddr}
	if joinAddr != "" {
		args = append(args, "--join", joinAddr)
	}
	logFile, err := os.OpenFile(n.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binPath, args...)
	cmd.Dir = n.dir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", n.id, err)
	}
	n.cmd = cmd
	t.Cleanup(func() { n.kill() })
}

func (n *node) kill() {
	if n.cmd != nil && n.cmd.Process != nil {
		n.cmd.Process.Kill()
		n.cmd.Wait()
		n.cmd = nil
	}
}

func (n *node) logTail(lines int) string {
	data, err := os.ReadFile(n.logPath)
	if err != nil {
		return fmt.Sprintf("(no log: %v)", err)
	}
	all := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(all) > lines {
		all = all[len(all)-lines:]
	}
	return strings.Join(all, "\n")
}

func dumpLogsOnFailure(t *testing.T, nodes []*node) {
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		for _, n := range nodes {
			t.Logf("---- %s log tail ----\n%s", n.id, n.logTail(25))
		}
	})
}

// ---------- http helpers ----------

func endpointOK(addr, path string) bool {
	resp, err := http.Get("http://" + addr + path)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func healthy(addr string) bool { return endpointOK(addr, "/healthz") }
func ready(addr string) bool   { return endpointOK(addr, "/readyz") }

func nodeStatus(addr string) (statusResponse, bool) {
	resp, err := http.Get("http://" + addr + "/v1/status")
	if err != nil {
		return statusResponse{}, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return statusResponse{}, false
	}
	var status statusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return statusResponse{}, false
	}
	return status, true
}

func hashOf(payload string) string {
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func putBlob(addr, payload string) (int, string) {
	hash := hashOf(payload)
	req, _ := http.NewRequest(http.MethodPut,
		"http://"+addr+"/v1/blob/"+hash, strings.NewReader(payload))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, hash
	}
	resp.Body.Close()
	return resp.StatusCode, hash
}

func getBlob(addr, hash string) (int, string) {
	resp, err := http.Get("http://" + addr + "/v1/blob/" + hash)
	if err != nil {
		return 0, ""
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func waitFor(t *testing.T, desc string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for: %s", timeout, desc)
}

// A write probe cannot identify the leader because followers forward writes.
func findLeader(t *testing.T, nodes []*node) *node {
	t.Helper()
	var leader *node
	waitFor(t, "a Raft leader to be elected", 20*time.Second, func() bool {
		for _, n := range nodes {
			if n.cmd == nil {
				continue
			}
			if status, ok := nodeStatus(n.httpAddr); ok && status.State == raft.Leader.String() {
				leader = n
				return true
			}
		}
		return false
	})
	return leader
}

// startCluster boots node1, waits until it leads, then joins node2 and node3.
func startCluster(t *testing.T) []*node {
	t.Helper()
	addrs := freeAddrs(t, 6)
	nodes := []*node{
		newNode(t, "node1", addrs[0], addrs[1]),
		newNode(t, "node2", addrs[2], addrs[3]),
		newNode(t, "node3", addrs[4], addrs[5]),
	}
	dumpLogsOnFailure(t, nodes)

	nodes[0].start(t, "")
	waitFor(t, "node1 healthy", 10*time.Second, func() bool { return healthy(nodes[0].httpAddr) })
	findLeader(t, nodes[:1])

	nodes[1].start(t, nodes[0].httpAddr)
	nodes[2].start(t, nodes[0].httpAddr)
	waitFor(t, "node2+node3 healthy", 10*time.Second, func() bool {
		return healthy(nodes[1].httpAddr) && healthy(nodes[2].httpAddr)
	})

	// Membership is proven when a fresh write on node1 becomes readable on both.
	status, hash := putBlob(nodes[0].httpAddr, `{"beignet":"membership-check"}`)
	if status != http.StatusOK {
		t.Fatalf("write to leader failed: HTTP %d", status)
	}
	waitFor(t, "membership-check blob replicated to node2+node3", 15*time.Second, func() bool {
		s2, _ := getBlob(nodes[1].httpAddr, hash)
		s3, _ := getBlob(nodes[2].httpAddr, hash)
		return s2 == http.StatusOK && s3 == http.StatusOK
	})
	return nodes
}

// ---------- tests ----------

func TestBlobReplication(t *testing.T) {
	nodes := startCluster(t)

	payload := `{"who":"wdree","test":"basic-replication"}`
	status, hash := putBlob(nodes[0].httpAddr, payload)
	if status != http.StatusOK {
		t.Fatalf("PUT to leader: HTTP %d", status)
	}
	for _, n := range nodes {
		n := n
		waitFor(t, n.id+" serves the blob", 10*time.Second, func() bool {
			s, body := getBlob(n.httpAddr, hash)
			return s == http.StatusOK && body == payload
		})
	}

	if s, _ := getBlob(nodes[1].httpAddr, hashOf("never-stored")); s != http.StatusNotFound {
		t.Errorf("unknown blob on node2: want 404, got %d", s)
	}
}

func TestLeaderFailoverAndCatchUp(t *testing.T) {
	nodes := startCluster(t)

	before := `{"written":"before-failover"}`
	status, beforeHash := putBlob(nodes[0].httpAddr, before)
	if status != http.StatusOK {
		t.Fatalf("PUT before failover: HTTP %d", status)
	}

	t.Log("killing node1 (leader)")
	nodes[0].kill()

	survivors := []*node{nodes[1], nodes[2]}
	newLeader := findLeader(t, survivors)
	t.Logf("new leader: %s", newLeader.id)

	after := `{"written":"after-failover"}`
	status, afterHash := putBlob(newLeader.httpAddr, after)
	if status != http.StatusOK {
		t.Fatalf("PUT to new leader %s: HTTP %d", newLeader.id, status)
	}
	for _, n := range survivors {
		n := n
		waitFor(t, n.id+" serves both blobs", 10*time.Second, func() bool {
			s1, b1 := getBlob(n.httpAddr, beforeHash)
			s2, b2 := getBlob(n.httpAddr, afterHash)
			return s1 == http.StatusOK && b1 == before && s2 == http.StatusOK && b2 == after
		})
	}

	t.Log("restarting node1 with its old data dir")
	nodes[0].start(t, "")
	waitFor(t, "node1 healthy again", 10*time.Second, func() bool { return healthy(nodes[0].httpAddr) })
	waitFor(t, "node1 caught up with entries from its downtime", 15*time.Second, func() bool {
		s, body := getBlob(nodes[0].httpAddr, afterHash)
		return s == http.StatusOK && body == after
	})
}
