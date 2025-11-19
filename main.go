// main.go
package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"
)

func main() {
	cfg := loadConfig()
	monitor := NewResourceMonitor(2 * time.Second)
	defer monitor.Stop()
	coreSnapshot := monitor.Snapshot()
	logInfof("Detected %d logical CPU cores (GOMAXPROCS=%d)", coreSnapshot.LogicalCores, runtime.GOMAXPROCS(0))

	// Inizializza l'engine del database
	engine, err := NewEngine(cfg, monitor)
	if err != nil {
		log.Fatalf("FATAL: Failed to start engine: %v", err)
	}
	defer engine.Close() // Assicura che tutti i DB siano chiusi all'uscita

	// Avvia il server TCP in una goroutine separata
	server := NewTCPServer(cfg.ListenAddr, engine)
	go func() {
		if err := server.Start(); err != nil {
			logErrorf("TCP Server failed: %v", err)
		}
	}()

	// Gestisce la chiusura pulita su segnali come Ctrl+C
	setupGracefulShutdown(engine, monitor)

	if os.Getenv("CHEETAH_HEADLESS") == "1" {
		logInfof("CheetahDB headless mode active. CLI disabled; press Ctrl+C to stop the server.")
		select {}
	}

	// Avvia l'interfaccia a riga di comando (CLI)
	runCLI(engine)
}

// runCLI gestisce l'input dal terminale per i comandi locali
func runCLI(engine *Engine) {
	logInfof("CheetahDB Console Interface is running. Type 'EXIT' to quit.")
	currentDB, err := engine.GetDatabase(engine.DefaultDatabaseName())
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
				engine.SetDatabaseOverrides(target, *overrides)
			}
			newDB, err := engine.GetDatabase(target)
			if err != nil {
				response = fmt.Sprintf("ERROR,cannot_load_db:%v", err)
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
				engine.SetDatabaseOverrides(target, *overrides)
			}
			if err := engine.ResetDatabase(target); err != nil {
				response = fmt.Sprintf("ERROR,cannot_reset_db:%v", err)
				break
			}
			newDB, err := engine.GetDatabase(target)
			if err != nil {
				response = fmt.Sprintf("ERROR,cannot_load_db:%v", err)
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
		fmt.Println(response)
	}
}

// setupGracefulShutdown attende un segnale di interruzione per chiudere le risorse
func setupGracefulShutdown(engine *Engine, monitor *ResourceMonitor) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		logInfof("Shutdown signal received. Closing resources...")
		engine.Close()
		if monitor != nil {
			monitor.Stop()
		}
		os.Exit(0)
	}()
}
