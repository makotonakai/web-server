package main

import (
	"fmt"
	"net"
	"bufio"
	"strings"
)


func main() {
	
	l, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println(err)
	}
	defer l.Close()

	for {

		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
		}

		go serveClient(conn)
		
	}
}

func serveClient(conn net.Conn) {

	reader := bufio.NewReader(conn)

	for {

		// GET / HTTP/1.1
		// Host: localhost:8080
		// User-Agent: curl/8.7.1
		// Accept: */*
		//
		
		msg, err := reader.ReadString('\n')

		if err != nil {
			continue
		}

		req := strings.Split(msg, " ")

		switch len(req) {

			case 1:
				continue

			default:

				method := req[0]
				path := req[1]
				
				if method == "GET" && path == "/ping" {
					conn.Write([]byte(
						"HTTP/1.1 200 OK\r\n" +
						"Content-Length: 4\r\n" +
						"Content-Type: text/plain\r\n" +
						"\r\n" +
						"pong",
					))
				}
		}
	}
}
