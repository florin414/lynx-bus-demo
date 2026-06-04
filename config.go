package main

import (
	"flag"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type runtimeConfig struct {
	listenAddr      string
	connectionLimit int
	acceptDeadline  time.Duration
	dataDir         string
}

const (
	ListenAddr             string        = "127.0.0.1:0"
	DefaultConnectionLimit int           = 10
	DefaultAcceptDeadline  time.Duration = 0
	DataDir                string        = "output/broker"

	EnvListenAddr      string = "LYNX_LISTEN_ADDR"
	EnvConnectionLimit string = "LYNX_CONNECTION_LIMIT"
	EnvAcceptDeadline  string = "LYNX_ACCEPT_DEADLINE"
	EnvDataDir         string = "LYNX_DATA_DIR"
)

func resolveRuntimeConfig() runtimeConfig {
	var cliListenAddr string
	var cliDataDir string
	var cliLimit int
	var cliDeadline time.Duration

	flag.StringVar(&cliListenAddr, "listen-addr", "", "broker listen address (overrides env)")
	flag.StringVar(&cliDataDir, "data-dir", "", "broker storage data directory (overrides env)")
	flag.IntVar(&cliLimit, "connections", 0, "maximum broker TCP connections (overrides env)")
	flag.DurationVar(&cliDeadline, "accept-deadline", 0, "broker accept deadline duration (overrides env)")
	flag.Parse()

	cfg := runtimeConfig{
		listenAddr:      ListenAddr,
		connectionLimit: DefaultConnectionLimit,
		acceptDeadline:  DefaultAcceptDeadline,
		dataDir:         DataDir,
	}

	if raw := strings.TrimSpace(os.Getenv(EnvListenAddr)); raw != "" {
		cfg.listenAddr = raw
	}
	if raw := strings.TrimSpace(os.Getenv(EnvDataDir)); raw != "" {
		cfg.dataDir = raw
	}

	if raw := strings.TrimSpace(os.Getenv(EnvConnectionLimit)); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			log.Printf("[config] invalid %s=%q, using default=%d", EnvConnectionLimit, raw, DefaultConnectionLimit)
		} else {
			cfg.connectionLimit = parsed
		}
	}

	if raw := strings.TrimSpace(os.Getenv(EnvAcceptDeadline)); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed < 0 {
			log.Printf("[config] invalid %s=%q, using default=%s", EnvAcceptDeadline, raw, DefaultAcceptDeadline)
		} else {
			cfg.acceptDeadline = parsed
		}
	}

	if isFlagPassed("listen-addr") {
		if raw := strings.TrimSpace(cliListenAddr); raw != "" {
			cfg.listenAddr = raw
		} else {
			log.Printf("[config] invalid -listen-addr=%q, using %s=%s", cliListenAddr, EnvListenAddr, cfg.listenAddr)
		}
	}

	if isFlagPassed("data-dir") {
		if raw := strings.TrimSpace(cliDataDir); raw != "" {
			cfg.dataDir = raw
		} else {
			log.Printf("[config] invalid -data-dir=%q, using %s=%s", cliDataDir, EnvDataDir, cfg.dataDir)
		}
	}

	if isFlagPassed("connections") {
		if cliLimit > 0 {
			cfg.connectionLimit = cliLimit
		} else {
			log.Printf("[config] invalid -connections=%d, using %s=%d", cliLimit, EnvConnectionLimit, cfg.connectionLimit)
		}
	}

	if isFlagPassed("accept-deadline") {
		if cliDeadline >= 0 {
			cfg.acceptDeadline = cliDeadline
		} else {
			log.Printf("[config] invalid -accept-deadline=%s, using %s=%s", cliDeadline, EnvAcceptDeadline, cfg.acceptDeadline)
		}
	}

	return cfg
}

func isFlagPassed(name string) bool {
	passed := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			passed = true
		}
	})

	return passed
}
