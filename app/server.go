package main

import (
	"flag"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type storeValue struct {
	value  string
	expiry int64 // Unix timestamp in milliseconds
}

var store = struct {
	sync.RWMutex
	m map[string]storeValue
}{m: make(map[string]storeValue)}

func main() {
	fmt.Println("Logs from your program will appear here!")

	port := flag.Int("port", 6379, "port to listen on")
	flag.Parse()

	address := fmt.Sprintf("0.0.0.0:%d", *port)
	l, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println("Failed to bind to port ", *port, ":", err)
		return
	}
	defer l.Close()

	go cleanupExpiredKeys()

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

	if len(parts) < 2 {
		return "-ERR invalid command\r\n"
	}

	numArgs, err := strconv.Atoi(strings.TrimPrefix(parts[0], "*"))
	if err != nil {
		return "-ERR invalid number of arguments\r\n"
	}

	if numArgs == 0 {
		return "-ERR invalid command\r\n"
	}

	command := strings.ToUpper(parts[2])
	args := parts[3:]
	fmt.Println(args)
	switch command {
	case "PING":
		return "+PONG\r\n"
	case "ECHO":
		if len(args) != 2 {
			return "-ERR wrong number of arguments for 'echo' command\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])
	case "SET":
		return handleSetCommand(args)
	case "GET":
		return handleGetCommand(args[1])
	case "INFO":
		if len(args) != 2 || strings.ToUpper(args[1]) != "REPLICATION" {
			return "-ERR wrong number of arguments for 'info' command\r\n"
		}
		return handleInfoCommand()
	default:
		return "-ERR unknown command\r\n"
	}
}

func handleSetCommand(args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'set' command\r\n"
	}
	fmt.Println(args)
	key, value := args[1], args[3]
	var expiry int64 = 0

	if len(args) == 8 && strings.ToUpper(args[5]) == "PX" {
		expiryMillis, err := strconv.ParseInt(args[7], 10, 64)
		if err != nil {
			return "-ERR invalid expiry time\r\n"
		}
		expiry = time.Now().UnixNano()/1e6 + expiryMillis
	}

	store.Lock()
	store.m[key] = storeValue{value: value, expiry: expiry}
	fmt.Println(store.m)
	store.Unlock()

	return "+OK\r\n"
}

func handleGetCommand(key string) string {
	store.RLock()
	defer store.RUnlock()
	fmt.Println(key)
	value, exists := store.m[key]
	if !exists || (value.expiry > 0 && value.expiry <= time.Now().UnixNano()/1e6) {
		return "$-1\r\n" // Null bulk string
	}

	return fmt.Sprintf("$%d\r\n%s\r\n", len(value.value), value.value)
}

func handleInfoCommand() string {
	info := "role:master\r\n"
	return fmt.Sprintf("$%d\r\n%s\r\n", len(info), info)
}

func cleanupExpiredKeys() {
	for {
		time.Sleep(100 * time.Millisecond)
		now := time.Now().UnixNano() / 1e6

		store.Lock()
		for key, value := range store.m {
			if value.expiry > 0 && value.expiry <= now {
				delete(store.m, key)
			}
		}
		store.Unlock()
	}
}
