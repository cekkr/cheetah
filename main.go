// main.go
package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

const (
	ListenAddr    = "0.0.0.0:4455"
	DataDir       = "cheetah_data"
	DefaultDbName = "default"
)

func main() {
	// Inizializza l'engine del database
	engine, err := NewEngine(DataDir)
	if err != nil {
		log.Fatalf("FATAL: Failed to start engine: %v", err)
	}
	defer engine.Close() // Assicura che tutti i DB siano chiusi all'uscita

	// Avvia il server TCP in una goroutine separata
	server := NewTCPServer(ListenAddr, engine)
	go func() {
		if err := server.Start(); err != nil {
			log.Printf("ERROR: TCP Server failed: %v", err)
		}
	}()

	// Gestisce la chiusura pulita su segnali come Ctrl+C
	setupGracefulShutdown(engine)

	// Avvia l'interfaccia a riga di comando (CLI)
	runCLI(engine)
}

// runCLI gestisce l'input dal terminale per i comandi locali
func runCLI(engine *Engine) {
	log.Println("CheetahDB Console Interface is running. Type 'EXIT' to quit.")
	currentDB, err := engine.GetDatabase(DefaultDbName)
	if err != nil {
		log.Fatalf("FATAL: Failed to load default database for CLI: %v", err)
		return
	}

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("[%s]> ", currentDB.Path())
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.ToUpper(line) == "EXIT" {
			break
		}

		var response string
		// Logica per cambiare DB nella CLI
		parts := strings.SplitN(line, " ", 2)
		if strings.ToUpper(parts[0]) == "DATABASE" && len(parts) > 1 {
			dbName := strings.TrimSpace(parts[1])
			newDB, err := engine.GetDatabase(dbName)
			if err != nil {
				response = fmt.Sprintf("ERROR,cannot_load_db:%v", err)
			} else {
				currentDB = newDB
				response = fmt.Sprintf("SUCCESS,database_changed_to_%s", dbName)
			}
		} else {
			// Esegue tutti gli altri comandi sul DB corrente
			response, err = currentDB.ExecuteCommand(line)
			if err != nil {
				response = fmt.Sprintf("ERROR,internal_error:%v", err)
			}
		}
		fmt.Println(response)
	}
}

// setupGracefulShutdown attende un segnale di interruzione per chiudere le risorse
func setupGracefulShutdown(engine *Engine) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("Shutdown signal received. Closing resources...")
		engine.Close()
		os.Exit(0)
	}()
}