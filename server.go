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
	logInfof("CheetahDB TCP server listening on %s", s.listenAddr)

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
	logInfof("New connection from %s", conn.RemoteAddr())
	defer conn.Close()

	currentDB, err := s.engine.GetDatabase(s.engine.DefaultDatabaseName())
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
				logErrorf("Reading from %s: %v", conn.RemoteAddr(), err)
			}
			break
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var response string
		parts := strings.SplitN(line, " ", 2)
		command := strings.ToUpper(parts[0])
		switch command {
		case "DATABASE":
			if len(parts) < 2 {
				response = "ERROR,missing_database_name"
				break
			}
			target, overrides, parseErr := parseDatabaseTarget(parts[1])
			if parseErr != nil {
				response = fmt.Sprintf("ERROR,%v", parseErr)
				break
			}
			if overrides != nil {
				s.engine.SetDatabaseOverrides(target, *overrides)
			}
			newDB, errDb := s.engine.GetDatabase(target)
			if errDb != nil {
				response = fmt.Sprintf("ERROR,cannot_load_db:%v", errDb)
			} else {
				currentDB = newDB
				response = fmt.Sprintf("SUCCESS,database_changed_to_%s", target)
			}
		case "RESET_DB":
			target := currentDB.Name()
			var overrides *DatabaseOverrides
			if len(parts) > 1 && strings.TrimSpace(parts[1]) != "" {
				var parseErr error
				target, overrides, parseErr = parseDatabaseTarget(parts[1])
				if parseErr != nil {
					response = fmt.Sprintf("ERROR,%v", parseErr)
					break
				}
			}
			if overrides != nil {
				s.engine.SetDatabaseOverrides(target, *overrides)
			}
			if err := s.engine.ResetDatabase(target); err != nil {
				response = fmt.Sprintf("ERROR,cannot_reset_db:%v", err)
				break
			}
			newDB, errDb := s.engine.GetDatabase(target)
			if errDb != nil {
				response = fmt.Sprintf("ERROR,cannot_load_db:%v", errDb)
			} else {
				currentDB = newDB
				response = fmt.Sprintf("SUCCESS,database_reset_to_%s", target)
			}
		default:
			response, err = currentDB.ExecuteCommand(line)
			if err != nil {
				response = fmt.Sprintf("ERROR,internal_error:%v", err)
			}
		}

		if _, err := io.WriteString(conn, response+"\n"); err != nil {
			logErrorf("Writing to %s: %v", conn.RemoteAddr(), err)
			break
		}
	}
	logInfof("Connection closed for %s", conn.RemoteAddr())
}
