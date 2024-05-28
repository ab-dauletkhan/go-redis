package main

import (
	"bufio"
	"encoding/hex"
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
	replicaPort      int
	emptyRDBHex      = "524544495330303036ff4a00"
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
		replicaPort = *port
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
			fmt.Println("Error connecting to master:", err)
			time.Sleep(2 * time.Second) // Retry after 2 seconds
			continue
		}

		defer conn.Close()

		writer := bufio.NewWriter(conn)
		reader := bufio.NewReader(conn)

		// Send PING command to master
		pingCommand := "*1\r\n$4\r\nPING\r\n"
		_, err = writer.WriteString(pingCommand)
		if err != nil {
			fmt.Println("Error sending PING to master:", err)
			return
		}
		writer.Flush()
		fmt.Println("PING command sent to master")

		// Read 1st response from master
		pingResponse, err := reader.ReadString('\n')
		if err != nil || !strings.HasPrefix(pingResponse, "+PONG") {
			fmt.Println("Error receiving PING response from master or invalid response:", err, pingResponse)
			return
		}

		// Send first REPLCONF command with listening-port
		replconfPortCommand := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n%d\r\n", replicaPort)
		_, err = writer.WriteString(replconfPortCommand)
		if err != nil {
			fmt.Println("Error sending REPLCONF listening-port to master:", err)
			return
		}
		writer.Flush()
		fmt.Println("REPLCONF listening-port command sent to master")

		// Read 2nd response from master
		replconfPortResponse, err := reader.ReadString('\n')
		if err != nil || !strings.HasPrefix(replconfPortResponse, "+OK") {
			fmt.Println("Error receiving REPLCONF listening-port response from master or invalid response:", err, replconfPortResponse)
			return
		}

		// Send second REPLCONF command with capa psync2
		replconfCapaCommand := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
		_, err = writer.Write([]byte(replconfCapaCommand))
		if err != nil {
			fmt.Println("Error sending REPLCONF capa psync2 to master:", err)
			return
		}
		writer.Flush()
		fmt.Println("REPLCONF capa psync2 command sent to master")

		// Read 3rd response from master
		replconfCapaResponse, err := reader.ReadString('\n')
		if err != nil || !strings.HasPrefix(replconfCapaResponse, "+OK") {
			fmt.Println("Error receiving REPLCONF capa psync2 response from master or invalid response:", err, replconfCapaResponse)
			return
		}

		// Send PSYNC command with ? -1
		psyncCommand := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
		_, err = writer.WriteString(psyncCommand)
		if err != nil {
			fmt.Println("Error sending PSYNC to master:", err)
			return
		}
		writer.Flush()
		fmt.Println("PSYNC command sent to master")

		// Read 4th response from master
		psyncResponse, err := reader.ReadString('\n')
		if err != nil || !strings.HasPrefix(psyncResponse, "+FULLRESYNC") {
			fmt.Println("Erorr receiving PSYNC response from master or invalid response:", err, psyncResponse)
			return
		}
		fmt.Println("PSYNC response:", psyncResponse)

		// Decode the empty RDB file from its hexadecimal representation
		emptyRDB, err := hex.DecodeString(emptyRDBHex)
		if err != nil {
			fmt.Println("Error decoding empty RDB file:", err)
			return
		}

		// Send the empty RDB file to the replica after receiving the PSYNC ? -1 command
		emptyRDBLength := len(emptyRDB)
		_, err = writer.WriteString(fmt.Sprintf("$%d\r\n", emptyRDBLength))
		if err != nil {
			fmt.Println("Error sending empty RDB file length to master:", err)
			return
		}
		_, err = writer.Write(emptyRDB)
		if err != nil {
			fmt.Println("Error sending empty RDB file contents to master:", err)
			return
		}
		writer.Flush()
		fmt.Printf("Empty RDB file (length %d) sent to master\n", emptyRDBLength)

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
		response := processCommand(request, conn)
		conn.Write([]byte(response))
	}
}

func processCommand(request string, conn net.Conn) string {
	trimmedString := strings.TrimSuffix(request, "\r\n")
	parts := strings.Split(trimmedString, "\r\n")
	fmt.Println(parts)
	if len(parts) < 2 {
		return "-ERR invalid command\r\n"
	}

	numArgs, err := strconv.Atoi(strings.TrimPrefix(parts[0], "*"))
	fmt.Println(numArgs)
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
		return handleInfoCommand(parts)
	case "REPLCONF":
		return "+OK\r\n"
	case "PSYNC":
		if numArgs != 3 || len(parts) < 7 {
			return "-ERR wrong number of arguments for 'psync' command\r\n"
		}
		// Send +FULLRESYNC response
		fullResyncResponse := fmt.Sprintf("+FULLRESYNC %s 0\r\n", masterReplId)
		conn.Write([]byte(fullResyncResponse))

		// Send the empty RDB file
		rdbContent, err := hex.DecodeString(emptyRDBHex)
		if err != nil {
			fmt.Println("Error decoding empty RDB hex:", err)
			return "-ERR internal error\r\n"
		}

		rdbResponse := fmt.Sprintf("$%d\r\n%s", len(rdbContent), rdbContent)
		conn.Write([]byte(rdbResponse))

		return ""
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
	store.Lock()
	defer store.Unlock()

	if value, ok := store.m[key]; ok {
		if value.expiry == 0 || value.expiry > time.Now().UnixNano()/1e6 {
			return fmt.Sprintf("$%d\r\n%s\r\n", len(value.value), value.value)
		}

		// Key has expired, delete it
		delete(store.m, key)
	}

	// Key not found or expired
	return "$-1\r\n"
}

func handleInfoCommand(args []string) string {
	role := "master"
	if isReplica {
		role = "slave"
	}
	fmt.Println(args)
	info := fmt.Sprintf("# Replication\r\nrole:%s\r\nconnected_slaves:0\r\nmaster_replid:%s\r\nmaster_repl_offset:%d\r\n", role, masterReplId, masterReplOffset)
	return fmt.Sprintf("$%d\r\n%s\r\n", len(info), info)
}

func cleanupExpiredKeys() {
	for {
		time.Sleep(time.Minute)
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
