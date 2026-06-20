package main

import (
	"fmt"
	"math/rand"
)

// LogRecord represents a single web API log entry for FTS benchmarking.
type LogRecord struct {
	Timestamp int64  // microseconds since epoch
	Level     string // DEBUG, INFO, WARN, ERROR, FATAL
	Service   string // api, auth, gateway, etc.
	Host      string // server001, server002, etc.
	Method    string // GET, POST, PUT, DELETE, PATCH
	Path      string // /api/v1/users, etc.
	Message   string // log message — primary FTS target
	DocID     string // unique document identifier for correlation
}

var (
	levels   = []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
	services = []string{
		"api", "auth", "gateway", "users", "orders", "payments", "inventory",
		"notifications", "search", "analytics", "reporting", "scheduler",
		"cache", "queue", "storage", "cdn", "mailer", "sms", "webhook", "audit",
	}
	methods = []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	paths   = []string{
		"/api/v1/users", "/api/v1/users/{id}", "/api/v1/orders", "/api/v1/orders/{id}",
		"/api/v1/products", "/api/v1/products/{id}", "/api/v1/cart", "/api/v1/checkout",
		"/api/v1/payments", "/api/v1/auth/login", "/api/v1/auth/logout", "/api/v1/auth/refresh",
		"/api/v1/search", "/api/v1/notifications", "/api/v1/webhooks", "/api/v1/health",
	}
	statusCodes = []int{200, 200, 200, 200, 201, 204, 400, 401, 403, 404, 500, 502, 503}
	errorMsgs   = []string{
		"Connection timeout after 30s",
		"Database connection failed: connection refused",
		"Invalid request payload: missing required field",
		"Authentication failed: invalid token",
		"Rate limit exceeded: 429 too many requests",
		"Service unavailable: upstream connection reset",
		"Internal server error: null pointer dereference",
		"Resource not found: no matching record",
		"Permission denied: insufficient privileges",
		"Data corruption detected in storage layer",
		"TLS handshake failed: certificate expired",
		"Memory allocation failed: out of memory",
	}
)

func generateRecords(n int, startTime int64) []LogRecord {
	records := make([]LogRecord, n)
	rng := rand.New(rand.NewSource(42)) // deterministic for reproducibility

	for i := 0; i < n; i++ {
		// Level distribution: 60% INFO, 25% DEBUG, 10% WARN, 4% ERROR, 1% FATAL
		var level string
		roll := rng.Float64()
		switch {
		case roll < 0.25:
			level = "DEBUG"
		case roll < 0.85:
			level = "INFO"
		case roll < 0.95:
			level = "WARN"
		case roll < 0.99:
			level = "ERROR"
		default:
			level = "FATAL"
		}

		service := services[rng.Intn(len(services))]
		method := methods[rng.Intn(len(methods))]
		path := paths[rng.Intn(len(paths))]
		statusCode := statusCodes[rng.Intn(len(statusCodes))]

		// Latency distribution
		var latency float64
		latRoll := rng.Float64()
		switch {
		case latRoll < 0.7:
			latency = 1 + rng.Float64()*50
		case latRoll < 0.9:
			latency = 50 + rng.Float64()*200
		case latRoll < 0.98:
			latency = 250 + rng.Float64()*750
		default:
			latency = 1000 + rng.Float64()*4000
		}

		var message string
		if level == "ERROR" || level == "FATAL" {
			errMsg := errorMsgs[rng.Intn(len(errorMsgs))]
			message = fmt.Sprintf("%s %s failed: %s", method, path, errMsg)
		} else {
			message = fmt.Sprintf("%s %s completed in %.0fms with status %d", method, path, latency, statusCode)
		}

		records[i] = LogRecord{
			Timestamp: startTime + int64(i),
			Level:     level,
			Service:   service,
			Host:      fmt.Sprintf("server%03d", rng.Intn(100)),
			Method:    method,
			Path:      path,
			Message:   message,
			DocID:     fmt.Sprintf("doc_%010d", i),
		}
	}

	return records
}
