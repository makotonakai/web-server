package main

import (
	"fmt"
	"net"
	"bufio"
	"strings"
)

const (
	StatusOK = "200 OK"
	StatusNotFound = "404 Not Found"
)

const (
	QUEUE_SIZE = 100
	WORKER_SIZE = 100
)

type Request struct {
	Method string
	Path string
	Version string
	Headers map[string]string
}


func main() {

	connChan := make(chan net.Conn, QUEUE_SIZE)

	for i := 0; i < WORKER_SIZE; i++ {
		go worker(connChan)
	}

	l, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println(err)
	}
	defer l.Close()


	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}

		connChan <- conn

	}
}

func worker(connChan <-chan net.Conn) {
	for conn := range connChan {
		serveClient(conn)
	}
}

func serveClient(conn net.Conn) {

	defer conn.Close()

	reader := bufio.NewReader(conn)

	raw, err := getRequest(reader)
	if err != nil {
		return // terminate connection if an error is caused
	}

	parsedReq, err := parseRequest(raw)
	if err != nil {
		return // terminate connection if an error is caused
	}

	if parsedReq.Method == "GET" && parsedReq.Path == "/ping" {
		writeResponse(conn, StatusOK, "pong")
	} else {
		writeResponse(conn, StatusNotFound, "Not Found")
	}

}

func getRequest(reader *bufio.Reader) (string, error) {

	req := strings.Builder{}

	for {

		line, err := reader.ReadString('\n')

		if err != nil {
			return "", fmt.Errorf("Client request error")
		}

		if line == "\r\n" {
			break // end of headers
		}

		req.WriteString(line)

	}

	return req.String(), nil
}

func parseRequest(raw string) (*Request, error) {

	lines := strings.Split(raw, "\r\n")
	if len(lines) < 1 {
		return nil, fmt.Errorf("Invalid request")
	}

	parts := strings.Split(lines[0], " ")
	req := &Request{
		Method: parts[0],
		Path: parts[1],
		Version: parts[2],
		Headers: make(map[string]string),
	}

	for _, line := range lines[1:] {

		kv := strings.SplitN(line, ":", 2)
		if len(kv) != 2 {
					continue
		}

		key := strings.TrimSpace(strings.ToLower(kv[0])) // case-insensitive
		value := strings.TrimSpace(kv[1])

		req.Headers[key] = value

	}

	return req, nil

}

func writeResponse(conn net.Conn, status string, body string) {
	
	resp := fmt.Sprintf(
		"HTTP/1.1 %s\r\nContent-Length: %d\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n%s",
		status,
		len(body),
		body,
	)

	conn.Write([]byte(resp))
}
