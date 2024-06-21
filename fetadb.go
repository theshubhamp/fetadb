package main

import (
	"fetadb/pkg/plan"
	"fetadb/pkg/sql"
	"fetadb/pkg/util"
	"flag"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	pgx "github.com/jackc/pgx/v5/pgproto3"
	pgquery "github.com/pganalyze/pg_query_go/v5"
	"log"
	"net"
)

var options struct {
	listenAddress string
	dbPath        string
}

func main() {
	flag.StringVar(&options.listenAddress, "listen", "127.0.0.1:5432", "Listen address")
	flag.StringVar(&options.dbPath, "dbpath", "memory", "Path to store DB, use 'memory' for non persistent mode")
	flag.Parse()

	listener, err := net.Listen("tcp", options.listenAddress)
	if err != nil {
		log.Fatalf("failed to setup listener: %v", err)
	}
	defer listener.Close()
	log.Printf("listening on %v", options.listenAddress)

	opt := badger.DefaultOptions("")
	if options.dbPath == "memory" {
		opt = opt.WithInMemory(true)
	} else {
		opt = opt.WithDir(options.dbPath).WithValueDir(options.dbPath)
	}
	log.Printf("db backed with %v", options.dbPath)

	db, err := badger.Open(opt)
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
		return
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("failed to accept connection: %v", err)
		}
		go handleIncomingConnection(db, conn)
	}
}

func handleIncomingConnection(db *badger.DB, conn net.Conn) {
	backend := pgx.NewBackend(conn, conn)

	msg, err := backend.ReceiveStartupMessage()
	if err != nil {
		log.Printf("failed to accept connection: %v", err)
		return
	}
	log.Printf("connection established from remote: %v", conn.RemoteAddr())

	if _, ok := msg.(*pgx.SSLRequest); ok {
		// deny ssl request
		_, err = conn.Write([]byte{'N'})
		if err != nil {
			log.Printf("failed to respond to ssl request: %v", err)
			return
		}

		// re-receive startup message after responding to ssl request
		_, err := backend.ReceiveStartupMessage()
		if err != nil {
			log.Printf("failed to accept connection: %v", err)
			return
		}
	} else if _, ok := msg.(*pgx.StartupMessage); ok {
		// got startup message, ok to proceed.
	} else {
		log.Printf("unsupported startup hanshake: %T", msg)
		return
	}

	backend.Send(&pgx.AuthenticationOk{})
	backend.Send(&pgx.ParameterStatus{Name: "server_version", Value: "16.0"})
	backend.Send(&pgx.BackendKeyData{ProcessID: 0, SecretKey: 0})
	backend.Send(&pgx.ReadyForQuery{TxStatus: 'I'})

	err = backend.Flush()
	if err != nil {
		log.Printf("failed to flush: %v", err)
		return
	}

	for {
		msg, err := backend.Receive()
		if err != nil {
			log.Printf("failed to receive message: %v", err)
			return
		}

		log.Printf("received message from connection: %T(%v)", msg, msg)
		handleMessage(db, backend, msg)
	}
}

func handleMessage(db *badger.DB, backend *pgx.Backend, msg pgx.FrontendMessage) {
	defer backend.Flush()

	switch msg := msg.(type) {
	case *pgx.Query:
		log.Printf("query: %v", msg.String)

		parseResult, err := pgquery.Parse(msg.String)
		if err != nil {
			err := fmt.Errorf("cannot parse: %v", err)
			backend.Send(&pgx.ErrorResponse{Message: err.Error()})
		} else {
			statements, err := sql.ToStatements(parseResult)
			if err != nil {
				err := fmt.Errorf("cannot convert pasre tree to ast: %v", err)
				backend.Send(&pgx.ErrorResponse{Message: err.Error()})
			} else {
				statement := statements[0]
				if selectStatement, ok := statement.(sql.Select); ok {
					planNode := plan.Select(selectStatement)
					result, err := planNode.Do(db)
					if err != nil {
						backend.Send(&pgx.ErrorResponse{Message: err.Error()})
					} else {
						backend.Send(util.ToRowDescription(result))
						for _, row := range util.ToDataRows(result) {
							backend.Send(&row)
						}
					}
				} else if createStatement, ok := statement.(sql.Create); ok {
					err := sql.CreateTable(db, createStatement)
					if err != nil {
						backend.Send(&pgx.ErrorResponse{Message: err.Error()})
					} else {
						backend.Send(util.ToRowDescription(util.DataFrame{}))
					}
				} else if insertStatement, ok := statement.(sql.Insert); ok {
					err := sql.InsertTable(db, insertStatement)
					if err != nil {
						backend.Send(&pgx.ErrorResponse{Message: err.Error()})
					}
				}
			}
		}

		backend.Send(&pgx.CommandComplete{})
		backend.Send(&pgx.ReadyForQuery{TxStatus: 'I'})
	}
}
