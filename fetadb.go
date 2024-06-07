package main

import (
	"flag"
	pgx "github.com/jackc/pgx/v5/pgproto3"
	_ "github.com/pganalyze/pg_query_go/v5"
	_ "github.com/syndtr/goleveldb/leveldb"
	"log"
	"net"
)

var options struct {
	listenAddress string
	remoteAddress string
}

func main() {
	flag.StringVar(&options.listenAddress, "listen", "127.0.0.1:5432", "Listen address")

	listener, err := net.Listen("tcp", options.listenAddress)
	if err != nil {
		log.Fatalf("failed to setup listener: %v", err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("failed to accept connection: %v", err)
		}
		go handleIncomingConnection(conn)
	}
}

func handleIncomingConnection(conn net.Conn) {
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
	}
}
