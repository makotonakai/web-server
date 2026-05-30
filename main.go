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
	// 負荷試験のパラメータの根拠
	QUEUE_SIZE  = 100 // request size?
	WORKER_SIZE = 100 // psで確認する
)

type Request struct {
	Method  string
	Path    string
	Version string
	Headers map[string]string
}

type Job struct {
	Conn     net.Conn
	QueuedAt time.Time
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
			Name: "http_request_duration_seconds",
			Help: "End-to-end request latency in seconds",
			Buckets: []float64{
				0.001,
				0.005,
				0.01,
				0.025,
				0.05,
				0.1,
				0.25,
				0.5,
				1,
				2,
				5,
			},
		},
		[]string{"method", "path"},
	)

	queueDelay = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: "connection_queue_delay_seconds",
			Help: "Time spent waiting in queue before worker pickup",
			Buckets: []float64{
				0.001,
				0.01,
				0.05,
				0.1,
				0.25,
				0.5,
				1,
				2,
				5,
			},
		},
	)

	acceptedConnections = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "accepted_connections_total",
			Help: "Total accepted TCP connections",
		},
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

	droppedConnections = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "dropped_connections_total",
			Help: "Connections dropped because queue is full",
		},
	)
)

func init() {

	prometheus.MustRegister(
		requestsTotal,
		requestDuration,
		queueDelay,
		acceptedConnections,
		activeConnections,
		connectionQueueDepth,
		queueUtilization,
		busyWorkers,
		requestErrors,
		droppedConnections,
	)

	// Initialize metrics with zero values
	requestsTotal.WithLabelValues(
		"GET",
		"/ping",
		StatusOK,
	)

	requestsTotal.WithLabelValues(
		"GET",
		"/ping",
		StatusNotFound,
	)

	requestDuration.WithLabelValues(
		"GET",
		"/ping",
	)
}

func main() {

	go func() {

		http.Handle("/metrics", promhttp.Handler())

		fmt.Println("Metrics server listening on :9091")

		err := http.ListenAndServe(":9091", nil)
		if err != nil {
			fmt.Println("Metrics server error:", err)
		}
	}()

	jobChan := make(chan Job, QUEUE_SIZE)

	for i := 0; i < WORKER_SIZE; i++ {
		go worker(jobChan)
	}

	// Periodic queue metrics updater
	go func() {

		ticker := time.NewTicker(100 * time.Millisecond)

		for range ticker.C {

			depth := len(jobChan)

			connectionQueueDepth.Set(float64(depth))

			queueUtilization.Set(
				float64(depth) / float64(QUEUE_SIZE),
			)
		}
	}()

	l, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer l.Close()

	fmt.Println("HTTP server listening on :8080")

	for {

		conn, err := l.Accept()
		acceptedConnections.Inc()

		if err != nil {
			fmt.Println("Accept error:", err)
			continue
		}

		job := Job{
			Conn:     conn,
			QueuedAt: time.Now(),
		}

		// Load shedding when queue is full
		select {

		case jobChan <- job:

		default:
			droppedConnections.Inc()
			conn.Close()
		}
	}
}

func worker(jobChan <-chan Job) {

	for job := range jobChan {

		busyWorkers.Inc()

		queueDelay.Observe(
			time.Since(job.QueuedAt).Seconds(),
		)

		serveClient(job.Conn)

		busyWorkers.Dec()
	}
}

func serveClient(conn net.Conn) {

	activeConnections.Inc()

	defer func() {
		activeConnections.Dec()
		conn.Close()
	}()

	conn.SetReadDeadline(
		time.Now().Add(5 * time.Second),
	)

	conn.SetWriteDeadline(
		time.Now().Add(5 * time.Second),
	)

	start := time.Now()

	// Artificial processing delay
	time.Sleep(500 * time.Millisecond)

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

	if parsedReq.Method == "GET" &&
		parsedReq.Path == "/ping" {

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
			return "", fmt.Errorf(
				"client request error: %w",
				err,
			)
		}

		req.WriteString(line)

		// End of HTTP headers
		if strings.TrimSpace(line) == "" {
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

func writeResponse(
	conn net.Conn,
	status string,
	body string,
) {

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
