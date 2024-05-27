package main

import (
	"fmt"
	"net"
	"strings"
)

var Storage map[string]string = make(map[string]string)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379:", err)
		return
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading from connection:", err)
			return
		}

		request := string(buf[:n])
		response := processCommand(request)
		conn.Write([]byte(response))
	}
}

func processCommand(request string) string {
	trimmedString := strings.TrimSuffix(request, "\r\n")
	parts := strings.Split(trimmedString, "\r\n")

	if len(parts) == 0 {
		return "-ERR invalid command\r\n"
	}
	fmt.Println(len(parts), parts)
	switch len(parts) {
	case 3:
		if strings.ToUpper(parts[2]) == "PING" { // Check for PING command
			return "+PONG\r\n"
		} else {
			return "-ERR unknown command after entering PING command\r\n"
		}
	case 5:
		if strings.ToUpper(parts[2]) == "ECHO" { // Check for ECHO command
			return fmt.Sprintf("$%d\r\n%s\r\n", len(parts[4]), parts[4])
		} else if strings.ToUpper(parts[2]) == "GET" { // If not ECHO, check for GET command
			key := parts[4]
			fmt.Println(key)
			val, prs := Storage[key]
			if prs {
				return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
			} else {
				return "$-1\r\n"
			}

		} else {
			return "-ERR unknown command after entering ECHO command\r\n"
		}
	case 7:
		// Check for SET command
		if strings.ToUpper(parts[2]) == "SET" {
			// Handle SET command
			key := parts[4]
			val := parts[6]
			fmt.Println(key, val)
			Storage[key] = val
			return "+OK\r\n"
		} else {
			return "-ERR unknown command after entering SET command\r\n"
		}
	default:
		return "-ERR unknown command\r\n"
	}
}
