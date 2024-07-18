package app

import (
	"fetadb/pkg/plan"
	"fetadb/pkg/sql"
	"fetadb/pkg/sql/stmt"
	"fetadb/pkg/util/types"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	pgproto "github.com/jackc/pgx/v5/pgproto3"
	pgquery "github.com/pganalyze/pg_query_go/v5"
	"log"
	"net"
	"sync"
)

type FetaDB struct {
	started  bool
	wg       *sync.WaitGroup
	listener net.Listener
}

func NewFetaDB() *FetaDB {
	wg := sync.WaitGroup{}

	return &FetaDB{
		started: false,
		wg:      &wg,
	}
}

func (feta *FetaDB) Start(listenAddr string, dbPath string) error {
	if feta.started {
		return fmt.Errorf("already started")
	}
	feta.started = true
	feta.wg.Add(1)

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Printf("failed to setup listener: %v", err)
		feta.started = false
		feta.wg.Done()
		return err
	}
	feta.listener = listener

	log.Printf("listening on %v", listenAddr)

	opt := badger.DefaultOptions("")
	if dbPath == "memory" {
		opt = opt.WithInMemory(true)
	} else {
		opt = opt.WithDir(dbPath).WithValueDir(dbPath)
	}
	log.Printf("db backed with %v", dbPath)

	db, err := badger.Open(opt)
	if err != nil {
		log.Printf("failed to open db: %v", err)
		feta.started = false
		feta.wg.Done()
		return err
	}

	go func() {
		defer feta.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("failed to accept connection: %v", err)
				feta.started = false
				return
			}
			go handleIncomingConnection(db, conn)
		}
	}()

	return nil
}

func (feta *FetaDB) Wait() {
	feta.wg.Wait()
}

func (feta *FetaDB) Stop() error {
	feta.started = false
	err := feta.listener.Close()

	if err != nil {
		return err
	}

	feta.Wait()

	return err
}

func handleIncomingConnection(db *badger.DB, conn net.Conn) {
	backend := pgproto.NewBackend(conn, conn)

	msg, err := backend.ReceiveStartupMessage()
	if err != nil {
		log.Printf("failed to accept connection: %v", err)
		return
	}
	log.Printf("connection established from remote: %v", conn.RemoteAddr())

	if _, ok := msg.(*pgproto.SSLRequest); ok {
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
	} else if _, ok := msg.(*pgproto.StartupMessage); ok {
		// got startup message, ok to proceed.
	} else {
		log.Printf("unsupported startup hanshake: %T", msg)
		return
	}

	backend.Send(&pgproto.AuthenticationOk{})
	backend.Send(&pgproto.ParameterStatus{Name: "server_version", Value: "16.0"})
	backend.Send(&pgproto.BackendKeyData{ProcessID: 0, SecretKey: 0})
	backend.Send(&pgproto.ReadyForQuery{TxStatus: 'I'})

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

func handleMessage(db *badger.DB, backend *pgproto.Backend, msg pgproto.FrontendMessage) {
	defer backend.Flush()

	switch msg := msg.(type) {
	case *pgproto.Query:
		log.Printf("query: %v", msg.String)

		parseResult, err := pgquery.Parse(msg.String)
		if err != nil {
			err := fmt.Errorf("cannot parse: %v", err)
			backend.Send(&pgproto.ErrorResponse{Message: err.Error()})
		} else {
			statements, err := sql.ToStatements(parseResult)
			if err != nil {
				err := fmt.Errorf("cannot convert pasre tree to ast: %v", err)
				backend.Send(&pgproto.ErrorResponse{Message: err.Error()})
			} else {
				statement := statements[0]
				if selectStatement, ok := statement.(stmt.Select); ok {
					planNode, err := plan.Select(selectStatement)
					if err != nil {
						backend.Send(&pgproto.ErrorResponse{Message: err.Error()})
					} else {
						result, err := planNode.Do(db)
						if err != nil {
							backend.Send(&pgproto.ErrorResponse{Message: err.Error()})
						} else {
							backend.Send(types.ToRowDescription(result))
							for _, row := range types.ToDataRows(result) {
								backend.Send(&row)
							}
						}
					}
				} else if createStatement, ok := statement.(stmt.Create); ok {
					err := stmt.CreateTable(db, createStatement)
					if err != nil {
						backend.Send(&pgproto.ErrorResponse{Message: err.Error()})
					}
				} else if insertStatement, ok := statement.(stmt.Insert); ok {
					err := stmt.InsertTable(db, insertStatement)
					if err != nil {
						backend.Send(&pgproto.ErrorResponse{Message: err.Error()})
					}
				}
			}
		}

		backend.Send(&pgproto.CommandComplete{})
		backend.Send(&pgproto.ReadyForQuery{TxStatus: 'I'})
	}
}
