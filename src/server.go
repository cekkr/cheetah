// server.go
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"
)

type TCPServer struct {
	listenAddr string
	engine     *Engine
	keepAlive  time.Duration
}

func NewTCPServer(listenAddr string, engine *Engine, keepAliveSeconds int) *TCPServer {
	var keepAlive time.Duration
	if keepAliveSeconds > 0 {
		keepAlive = time.Duration(keepAliveSeconds) * time.Second
	}
	return &TCPServer{
		listenAddr: listenAddr,
		engine:     engine,
		keepAlive:  keepAlive,
	}
}

func (s *TCPServer) Start() error {
	var listener net.Listener
	var err error
	if s.keepAlive > 0 {
		var lc net.ListenConfig
		lc.KeepAlive = s.keepAlive
		listener, err = lc.Listen(context.Background(), "tcp", s.listenAddr)
	} else {
		listener, err = net.Listen("tcp", s.listenAddr)
	}
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

// connSession è lo stato di scope connessione: il database corrente, che
// DATABASE e RESET_DB spostano. Esiste perché i due front-end della connessione
// TCP — testo e binario — devono instradare esattamente allo stesso modo: una
// seconda copia dello smistamento divergerebbe in silenzio, com'è già successo
// fra CLI e TCP.
type connSession struct {
	engine *Engine
	db     *Database
}

// execute instrada una riga di comando canonica. È l'unico punto in cui si
// decide chi esegue cosa: i comandi di scope engine, i due di scope connessione
// e tutto il resto a ExecuteCommand.
func (c *connSession) execute(line string) string {
	parts := strings.SplitN(line, " ", 2)
	command := strings.ToUpper(parts[0])
	controlArgs := ""
	if len(parts) > 1 {
		controlArgs = parts[1]
	}
	// I comandi di scope engine (DB_CREATE, DB_LIST) stanno in engine.go, in
	// una funzione condivisa con la CLI: non toccano il database corrente,
	// ma non possono nemmeno passare da ExecuteCommand.
	if handled, ok := engineControlCommand(c.engine, command, controlArgs); ok {
		return handled
	}

	switch command {
	case "DATABASE":
		if len(parts) < 2 {
			return "ERROR,missing_database_name"
		}
		target, overrides, parseErr := parseDatabaseTarget(parts[1])
		if parseErr != nil {
			return fmt.Sprintf("ERROR,%v", parseErr)
		}
		if overrides != nil {
			c.engine.SetDatabaseOverrides(target, *overrides)
		}
		newDB, errDb := c.engine.GetDatabase(target)
		if errDb != nil {
			return fmt.Sprintf("ERROR,cannot_load_db:%v", errDb)
		}
		c.db = newDB
		return fmt.Sprintf("SUCCESS,database_changed_to_%s", target)
	case "RESET_DB":
		target := c.db.Name()
		var overrides *DatabaseOverrides
		if len(parts) > 1 && strings.TrimSpace(parts[1]) != "" {
			var parseErr error
			target, overrides, parseErr = parseDatabaseTarget(parts[1])
			if parseErr != nil {
				return fmt.Sprintf("ERROR,%v", parseErr)
			}
		}
		if overrides != nil {
			c.engine.SetDatabaseOverrides(target, *overrides)
		}
		if err := c.engine.ResetDatabase(target); err != nil {
			return fmt.Sprintf("ERROR,cannot_reset_db:%v", err)
		}
		newDB, errDb := c.engine.GetDatabase(target)
		if errDb != nil {
			return fmt.Sprintf("ERROR,cannot_load_db:%v", errDb)
		}
		c.db = newDB
		return fmt.Sprintf("SUCCESS,database_reset_to_%s", target)
	default:
		response, err := c.db.ExecuteCommand(line)
		if err != nil {
			return fmt.Sprintf("ERROR,internal_error:%v", err)
		}
		return response
	}
}

func (s *TCPServer) handleConnection(conn net.Conn) {
	if s.keepAlive > 0 {
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			if err := tcpConn.SetKeepAlive(true); err != nil {
				log.Printf("WARN: Unable to enable TCP keep-alive for %s: %v", conn.RemoteAddr(), err)
			} else if err := tcpConn.SetKeepAlivePeriod(s.keepAlive); err != nil {
				log.Printf("WARN: Unable to set TCP keep-alive period for %s: %v", conn.RemoteAddr(), err)
			}
		}
	}
	logInfof("New connection from %s", conn.RemoteAddr())
	defer conn.Close()

	currentDB, err := s.engine.GetDatabase(s.engine.DefaultDatabaseName())
	if err != nil {
		io.WriteString(conn, "ERROR,failed_to_load_default_db\n")
		return
	}

	session := &connSession{engine: s.engine, db: currentDB}
	reader := bufio.NewReader(conn)

	// Un frame binario comincia con 0xC7, byte che nessun comando testuale può
	// avere in prima posizione: la modalità si riconosce dal primo byte, senza
	// negoziazione preventiva e senza rompere i client che parlano testo.
	if head, peekErr := reader.Peek(1); peekErr == nil && head[0] == binaryFrameMagic {
		s.handleBinaryConnection(conn, reader, session)
		logInfof("Connection closed for %s", conn.RemoteAddr())
		return
	}

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

		if _, err := io.WriteString(conn, session.execute(line)+"\n"); err != nil {
			logErrorf("Writing to %s: %v", conn.RemoteAddr(), err)
			break
		}
	}
	logInfof("Connection closed for %s", conn.RemoteAddr())
}

// handleBinaryConnection serve una connessione in modalità binaria
// (binary_protocol.go). L'handshake è obbligatorio e viene per primo: è lì che
// si fissano le larghezze di default dei tipi numerici per tutta la
// connessione.
//
// Un frame malformato non chiude la connessione — risponde con un ERROR come
// farebbe un comando sbagliato in testo. Solo un frame illeggibile a livello di
// intestazione (magic sbagliato, lunghezza fuori scala) la chiude, perché a
// quel punto non si sa più dove finisce un frame e dove comincia il prossimo.
func (s *TCPServer) handleBinaryConnection(conn net.Conn, reader *bufio.Reader, session *connSession) {
	binSession := newBinarySession()

	frame, err := readBinaryFrame(reader)
	if err != nil {
		logErrorf("Binary handshake from %s: %v", conn.RemoteAddr(), err)
		return
	}
	if frame.Type != binaryFrameHandshake {
		writeBinaryError(conn, "handshake_expected")
		return
	}
	version, widths, err := decodeHandshake(frame.Body)
	if err != nil {
		writeBinaryError(conn, sanitizeResponseToken(err.Error()))
		return
	}
	if version != binaryProtocolVersion {
		writeBinaryError(conn, fmt.Sprintf("unsupported_protocol_version:%d", version))
		return
	}
	binSession.widths = widths.overlay(defaultNumericProfile())

	if _, err := conn.Write(encodeHandshakeAck(binSession, currentCommandIndex(), currentArgumentKeys())); err != nil {
		logErrorf("Writing to %s: %v", conn.RemoteAddr(), err)
		return
	}
	logInfof("Binary protocol enabled for %s (uint=%d int=%d float=%d)",
		conn.RemoteAddr(), binSession.widths.Uint, binSession.widths.Int, binSession.widths.Float)

	for {
		frame, err := readBinaryFrame(reader)
		if err != nil {
			if err != io.EOF {
				logErrorf("Reading binary frame from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}
		if frame.Type != binaryFrameRequest {
			if !writeBinaryError(conn, "request_frame_expected") {
				return
			}
			continue
		}
		line, decodeErr := decodeBinaryRequest(session.db, binSession, frame.Body)
		if decodeErr != nil {
			if !writeBinaryError(conn, sanitizeResponseToken(decodeErr.Error())) {
				return
			}
			continue
		}
		if strings.TrimSpace(line) == "" {
			if !writeBinaryError(conn, "empty_command") {
				return
			}
			continue
		}
		response := session.execute(line)
		if _, err := conn.Write(encodeBinaryResponse(response, binSession.widths)); err != nil {
			logErrorf("Writing to %s: %v", conn.RemoteAddr(), err)
			return
		}
	}
}

// writeBinaryError risponde con un ERROR incorniciato. Rende false quando la
// scrittura fallisce, cioè quando il chiamante deve smettere.
func writeBinaryError(conn net.Conn, reason string) bool {
	_, err := conn.Write(encodeBinaryResponse("ERROR,"+reason, defaultNumericProfile()))
	if err != nil {
		logErrorf("Writing to %s: %v", conn.RemoteAddr(), err)
		return false
	}
	return true
}
