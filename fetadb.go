package main

import (
	"fetadb/pkg/app"
	"flag"
	"log"
)

func main() {
	var options struct {
		listenAddress string
		dbPath        string
	}

	flag.StringVar(&options.listenAddress, "listen", "127.0.0.1:5432", "Listen address")
	flag.StringVar(&options.dbPath, "dbpath", "memory", "Path to store DB, use 'memory' for non persistent mode")
	flag.Parse()

	feta := app.NewFetaDB()
	err := feta.Start(options.listenAddress, options.dbPath)
	if err != nil {
		log.Printf("failed to start fetadb: %v", err)
		return
	}
	feta.Wait()
}
