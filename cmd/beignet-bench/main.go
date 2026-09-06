package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const outputVersion = 1

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	var err error
	switch os.Args[1] {
	case "run":
		err = runCommand(os.Args[2:])
	case "worker":
		err = workerCommand(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "beignet-bench:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  beignet-bench run --targets URL[,URL...] [options]
  beignet-bench worker --targets URL[,URL...] --run RUN [options]

"run" submits deterministic one-step turns, optionally executes them with
in-process synthetic workers, and audits every committed result. "worker"
runs only synthetic executors so worker capacity can be placed independently.`)
	os.Exit(2)
}

func runCommand(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	var rawTargets string
	var cfg runConfig
	fs.StringVar(&rawTargets, "targets", "", "comma-separated Beignet HTTP base URLs")
	fs.StringVar(&cfg.runID, "run", "", "run identifier (generated when omitted)")
	fs.IntVar(&cfg.turns, "turns", 1_000, "number of one-step turns")
	fs.IntVar(&cfg.submitConcurrency, "submit-concurrency", 64, "parallel submitters")
	fs.IntVar(&cfg.submitBatchSize, "submit-batch-size", 1, "steps per submit request")
	fs.IntVar(&cfg.workerConcurrency, "workers", 32, "in-process synthetic workers; zero uses an external fleet")
	fs.IntVar(&cfg.workerBatchSize, "worker-batch-size", 1, "worker slots per claim and commit request")
	fs.DurationVar(&cfg.timeout, "timeout", 2*time.Minute, "whole-run deadline")
	fs.DurationVar(&cfg.requestTimeout, "request-timeout", 5*time.Second, "per-request deadline")
	fs.DurationVar(&cfg.auditInterval, "audit-interval", 250*time.Millisecond, "completion audit interval")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	var err error
	cfg.targets, err = parseTargets(rawTargets)
	if err != nil {
		return err
	}
	if cfg.runID == "" {
		cfg.runID, err = newRunID()
		if err != nil {
			return err
		}
	}
	if err := cfg.validate(); err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	summary, runErr := executeRun(ctx, cfg)
	if runErr != nil {
		summary.Error = runErr.Error()
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(summary); err != nil {
		return fmt.Errorf("encode summary: %w", err)
	}
	return runErr
}

func workerCommand(args []string) error {
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	var rawTargets string
	var cfg workerConfig
	fs.StringVar(&rawTargets, "targets", "", "comma-separated Beignet HTTP base URLs")
	fs.StringVar(&cfg.runID, "run", "", "benchmark run identifier")
	fs.StringVar(&cfg.workerPrefix, "worker-prefix", "", "stable worker ID prefix")
	fs.IntVar(&cfg.concurrency, "concurrency", 32, "parallel synthetic workers")
	fs.IntVar(&cfg.batchSize, "batch-size", 1, "worker slots per claim and commit request")
	fs.DurationVar(&cfg.duration, "duration", 30*time.Minute, "maximum worker lifetime")
	fs.DurationVar(&cfg.requestTimeout, "request-timeout", 5*time.Second, "per-request deadline")
	fs.DurationVar(&cfg.pollInterval, "poll-interval", 10*time.Millisecond, "delay after an empty claim")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	var err error
	cfg.targets, err = parseTargets(rawTargets)
	if err != nil {
		return err
	}
	if err := cfg.validate(); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, cfg.duration)
	defer cancel()
	group := startWorkers(ctx, cfg)
	select {
	case <-ctx.Done():
	case <-group.fatal:
	}
	cancel()
	group.wait()
	stats := group.stats
	if err := json.NewEncoder(os.Stdout).Encode(workerSummary{
		Version:   outputVersion,
		RunID:     cfg.runID,
		BatchSize: cfg.batchSize,
		Stats:     stats.snapshot(),
	}); err != nil {
		return fmt.Errorf("encode worker summary: %w", err)
	}
	if stats.fatal.Load() > 0 {
		return errors.New("synthetic worker encountered fatal claim or commit errors")
	}
	return nil
}
