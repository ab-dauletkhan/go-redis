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

var (
	store = struct {
		sync.RWMutex
		m map[string]storeValue
	}{m: make(map[string]storeValue)}
	isReplica        bool
	masterReplId     = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	masterReplOffset int64
	masterAddress    string
	masterPort       int
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	port := flag.Int("port", 6379, "port to listen on")
	replicaOf := flag.String("replicaof", "", "host and port of the master in the format 'host port'")
	flag.Parse()

	address := fmt.Sprintf("0.0.0.0:%d", *port)
	l, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println("Failed to bind to port", *port, ":", err)
		return
	}
	defer l.Close()

	if *replicaOf != "" {
		isReplica = true
		parts := strings.Split(*replicaOf, " ")
		masterAddress = parts[0]

		masterPort, err = strconv.Atoi(parts[1])
		if err != nil {
			fmt.Println("Invalid master port:", parts[1])
			return
		}
		go connectToMaster(masterAddress, masterPort)
	}

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

func connectToMaster(address string, port int) {
	for {
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", address, port))
		if err != nil {
			fmt.Sprintln("Error connecting to master:", err)
			time.Sleep(2 * time.Second) // Retry after 2 seconds
			continue
		}

		defer conn.Close()

		// Send PING command to master
		pingCommand := "*1\r\n$4\r\nPING\r\n"
		_, err = conn.Write([]byte(pingCommand))
		if err != nil {
			fmt.Println("Error sending PING to master:", err)
			return
		}
		fmt.Println("PING command sent to master")
		return
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
	fmt.Println(parts)
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
	fmt.Println(command)

	switch command {
	case "PING":
		return "+PONG\r\n"
	case "ECHO":
		if len(parts) != 5 {
			return "-ERR wrong number of arguments for 'echo' command\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(parts[4]), parts[4])
	case "SET":
		return handleSetCommand(parts)
	case "GET":
		return handleGetCommand(parts[4])
	case "INFO":
		// if len(args) != 2 || strings.ToUpper(args[1]) != "REPLICATION" {
		// 	return "-ERR wrong number of arguments for 'info' command\r\n"
		// }
		return handleInfoCommand(parts)
	default:
		return "-ERR unknown command\r\n"
	}
}

func handleSetCommand(args []string) string {
	if len(args) < 4 {
		return "-ERR wrong number of arguments for 'set' command\r\n"
	}
	fmt.Println(args)
	key, value := args[4], args[6]
	var expiry int64 = 0

	if len(args) == 11 && strings.ToUpper(args[8]) == "PX" {
		expiryMillis, err := strconv.ParseInt(args[10], 10, 64)
		if err != nil {
			return "-ERR invalid expiry time\r\n"
		}
		expiry = time.Now().UnixNano()/1e6 + expiryMillis
	}
	fmt.Println(key, value, expiry)
	store.Lock()
	store.m[key] = storeValue{value: value, expiry: expiry}
	fmt.Println(store.m)
	masterReplOffset += int64(len(value))
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

func handleInfoCommand(args []string) string {
	role := "master"
	if isReplica {
		role = "slave"
	}
	fmt.Println(args)
	info := fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d\r\n", role, masterReplId, masterReplOffset)
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
