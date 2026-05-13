package main

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	StatusOK       = "200 OK"
	StatusNotFound = "404 Not Found"
)

const (
	QUEUE_SIZE  = 100
	WORKER_SIZE = 100
)

type Request struct {
	Method  string
	Path    string
	Version string
	Headers map[string]string
}

var (
	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Request latency in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	activeConnections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "active_connections",
			Help: "Current active TCP connections",
		},
	)

	connectionQueueDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "connection_queue_depth",
			Help: "Current connection queue depth",
		},
	)

	queueUtilization = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "connection_queue_utilization_ratio",
			Help: "Connection queue utilization ratio",
		},
	)

	busyWorkers = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "busy_workers",
			Help: "Number of busy workers",
		},
	)

	requestErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "request_errors_total",
			Help: "Total request parsing/processing errors",
		},
	)
)

func init() {

	prometheus.MustRegister(
		requestsTotal,
		requestDuration,
		activeConnections,
		connectionQueueDepth,
		queueUtilization,
		busyWorkers,
		requestErrors,
	)
}

func main() {

	// Prometheus metrics endpoint
	go func() {
		http.Handle("/metrics", promhttp.Handler())

		fmt.Println("Metrics server listening on :9091")

		err := http.ListenAndServe(":9091", nil)
		if err != nil {
			fmt.Println("Metrics server error:", err)
		}
	}()

	connChan := make(chan net.Conn, QUEUE_SIZE)

	for i := 0; i < WORKER_SIZE; i++ {
		go worker(connChan)
	}

	l, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer l.Close()

	fmt.Println("HTTP server listening on :8080")

	for {

		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Accept error:", err)
			continue
		}

		connChan <- conn

		connectionQueueDepth.Set(float64(len(connChan)))

		queueUtilization.Set(
			float64(len(connChan)) / float64(QUEUE_SIZE),
		)
	}
}

func worker(connChan <-chan net.Conn) {

	for conn := range connChan {

		busyWorkers.Inc()

		serveClient(conn)

		busyWorkers.Dec()

		connectionQueueDepth.Set(float64(len(connChan)))

		queueUtilization.Set(
			float64(len(connChan)) / float64(QUEUE_SIZE),
		)
	}
}

func serveClient(conn net.Conn) {

	activeConnections.Inc()

	defer func() {
		activeConnections.Dec()
		conn.Close()
	}()

	start := time.Now()

	reader := bufio.NewReader(conn)

	raw, err := getRequest(reader)
	if err != nil {
		requestErrors.Inc()
		return
	}

	parsedReq, err := parseRequest(raw)
	if err != nil {
		requestErrors.Inc()
		return
	}

	status := StatusNotFound
	body := "Not Found"

	if parsedReq.Method == "GET" && parsedReq.Path == "/ping" {
		status = StatusOK
		body = "pong"
	}

	writeResponse(conn, status, body)

	requestsTotal.WithLabelValues(
		parsedReq.Method,
		parsedReq.Path,
		status,
	).Inc()

	requestDuration.WithLabelValues(
		parsedReq.Method,
		parsedReq.Path,
	).Observe(time.Since(start).Seconds())
}

func getRequest(reader *bufio.Reader) (string, error) {

	req := strings.Builder{}

	for {

		line, err := reader.ReadString('\n')

		if err != nil {
			return "", fmt.Errorf("client request error")
		}

		req.WriteString(line)

		if line == "\r\n" {
			break
		}
	}

	return req.String(), nil
}

func parseRequest(raw string) (*Request, error) {

	lines := strings.Split(raw, "\r\n")

	if len(lines) < 1 {
		return nil, fmt.Errorf("invalid request")
	}

	parts := strings.Split(lines[0], " ")

	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid request line")
	}

	req := &Request{
		Method:  parts[0],
		Path:    parts[1],
		Version: parts[2],
		Headers: make(map[string]string),
	}

	for _, line := range lines[1:] {

		if line == "" {
			continue
		}

		kv := strings.SplitN(line, ":", 2)

		if len(kv) != 2 {
			continue
		}

		key := strings.TrimSpace(
			strings.ToLower(kv[0]),
		)

		value := strings.TrimSpace(kv[1])

		req.Headers[key] = value
	}

	return req, nil
}

func writeResponse(conn net.Conn, status string, body string) {

	resp := fmt.Sprintf(
		"HTTP/1.1 %s\r\n"+
			"Content-Length: %d\r\n"+
			"Content-Type: text/plain\r\n"+
			"Connection: close\r\n\r\n%s",
		status,
		len(body),
		body,
	)

	_, err := conn.Write([]byte(resp))
	if err != nil {
		fmt.Println("Write error:", err)
	}
}
