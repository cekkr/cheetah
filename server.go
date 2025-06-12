// server.go
package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
)

type TCPServer struct {
	listenAddr string
	engine     *Engine
}

func NewTCPServer(listenAddr string, engine *Engine) *TCPServer {
	return &TCPServer{
		listenAddr: listenAddr,
		engine:     engine,
	}
}

func (s *TCPServer) Start() error {
	listener, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return err
	}
	defer listener.Close()
	log.Printf("CheetahDB TCP server listening on %s", s.listenAddr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("ERROR: Accepting connection: %v", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *TCPServer) handleConnection(conn net.Conn) {
	log.Printf("INFO: New connection from %s", conn.RemoteAddr())
	defer conn.Close()

	currentDB, err := s.engine.GetDatabase(DefaultDbName)
	if err != nil {
		io.WriteString(conn, "ERROR,failed_to_load_default_db\n")
		return
	}

	reader := bufio.NewReader(conn)
	for {
		// Non scriviamo un prompt via TCP, il client dovrebbe sapere cosa fare
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("ERROR: Reading from %s: %v", conn.RemoteAddr(), err)
			}
			break
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var response string
		parts := strings.SplitN(line, " ", 2)
		if strings.ToUpper(parts[0]) == "DATABASE" && len(parts) > 1 {
			dbName := strings.TrimSpace(parts[1])
			newDB, errDb := s.engine.GetDatabase(dbName)
			if errDb != nil {
				response = fmt.Sprintf("ERROR,cannot_load_db:%v", errDb)
			} else {
				currentDB = newDB
				response = fmt.Sprintf("SUCCESS,database_changed_to_%s", dbName)
			}
		} else {
			response, err = currentDB.ExecuteCommand(line)
			if err != nil {
				response = fmt.Sprintf("ERROR,internal_error:%v", err)
			}
		}

		if _, err := io.WriteString(conn, response+"\n"); err != nil {
			log.Printf("ERROR: Writing to %s: %v", conn.RemoteAddr(), err)
			break
		}
	}
	log.Printf("INFO: Connection closed for %s", conn.RemoteAddr())
}