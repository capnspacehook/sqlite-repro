package main

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"log"
	"net/netip"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

const pragmas = `
PRAGMA foreign_keys = true;
PRAGMA busy_timeout = 1000;
PRAGMA journal_mode = WAL;
`

//go:embed schema.sql
var schema string

var (
	cont1ID   = "container_one_ID"
	cont1Name = "container1"
	cont1Addr = netip.MustParseAddr("172.0.1.2")
)

func handleErr(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	tempDir, err := os.MkdirTemp("", "")
	handleErr(err)
	defer func() {
		os.RemoveAll(tempDir)
	}()

	dbFile := filepath.Join(tempDir, "sqlite.db")
	dbFile += "?_txlock=immediate"
	sqlDB, err := sql.Open("sqlite", dbFile)
	handleErr(err)
	defer sqlDB.Close()

	_, err = sqlDB.Exec(schema)
	handleErr(err)
	_, err = sqlDB.Exec(pragmas)
	handleErr(err)

	db, err := NewDB(context.Background(), sqlDB)
	handleErr(err)
	defer db.Close()

	tx, err := db.Begin(context.Background())
	handleErr(err)

	err = tx.AddContainer(context.Background(), "dummyid", "dummyname")
	handleErr(err)
	err = tx.DeleteContainer(context.Background(), "dummyid")
	handleErr(err)

	handleErr(tx.Commit())
	tx.Rollback()

	ctx, cancel := context.WithCancel(context.Background())
	tx, err = db.Begin(ctx)
	handleErr(err)

	err = tx.AddContainer(ctx, cont1ID, cont1Name)
	handleErr(err)
	err = tx.AddContainerAddr(ctx, cont1Addr.AsSlice(), cont1ID)
	handleErr(err)
	err = tx.AddContainerAlias(ctx, cont1ID, "/"+cont1Name)
	handleErr(err)

	// cancel the context so the committing the transaction will fail and rollback
	cancel()
	err = tx.Commit()
	if !errors.Is(err, context.Canceled) {
		handleErr(err)
	}
	tx.Rollback()

	err = db.GetContainers(context.Background())
	handleErr(err)
}

type DBTX interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	PrepareContext(context.Context, string) (*sql.Stmt, error)
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}

func Prepare(ctx context.Context, db *sql.DB) (*Queries, error) {
	q := Queries{db: db}
	var err error
	if q.addContainerStmt, err = db.PrepareContext(ctx, addContainer); err != nil {
		return nil, fmt.Errorf("error preparing query AddContainer: %w", err)
	}
	if q.addContainerAddrStmt, err = db.PrepareContext(ctx, addContainerAddr); err != nil {
		return nil, fmt.Errorf("error preparing query AddContainerAddr: %w", err)
	}
	if q.addContainerAliasStmt, err = db.PrepareContext(ctx, addContainerAlias); err != nil {
		return nil, fmt.Errorf("error preparing query AddContainerAlias: %w", err)
	}
	if q.deleteContainerStmt, err = db.PrepareContext(ctx, deleteContainer); err != nil {
		return nil, fmt.Errorf("error preparing query DeleteContainer: %w", err)
	}
	if q.getContainersStmt, err = db.PrepareContext(ctx, getContainers); err != nil {
		return nil, fmt.Errorf("error preparing query GetContainers: %w", err)
	}
	return &q, nil
}

func (q *Queries) Close() error {
	var err error
	if q.addContainerStmt != nil {
		if cerr := q.addContainerStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing addContainerStmt: %w", cerr)
		}
	}
	if q.addContainerAddrStmt != nil {
		if cerr := q.addContainerAddrStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing addContainerAddrStmt: %w", cerr)
		}
	}
	if q.addContainerAliasStmt != nil {
		if cerr := q.addContainerAliasStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing addContainerAliasStmt: %w", cerr)
		}
	}
	if q.deleteContainerStmt != nil {
		if cerr := q.deleteContainerStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing deleteContainerStmt: %w", cerr)
		}
	}
	if q.getContainersStmt != nil {
		if cerr := q.getContainersStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing getContainersStmt: %w", cerr)
		}
	}
	return err
}

func (q *Queries) exec(ctx context.Context, stmt *sql.Stmt, query string, args ...interface{}) (sql.Result, error) {
	switch {
	case stmt != nil && q.tx != nil:
		return q.tx.StmtContext(ctx, stmt).ExecContext(ctx, args...)
	case stmt != nil:
		return stmt.ExecContext(ctx, args...)
	default:
		return nil, errors.New("no prepared statement provided")
	}
}

func (q *Queries) query(ctx context.Context, stmt *sql.Stmt, query string, args ...interface{}) (*sql.Rows, error) {
	switch {
	case stmt != nil && q.tx != nil:
		return q.tx.StmtContext(ctx, stmt).QueryContext(ctx, args...)
	case stmt != nil:
		return stmt.QueryContext(ctx, args...)
	default:
		return nil, errors.New("no prepared statement provided")
	}
}

type Queries struct {
	db                    DBTX
	tx                    *sql.Tx
	addContainerStmt      *sql.Stmt
	addContainerAddrStmt  *sql.Stmt
	addContainerAliasStmt *sql.Stmt
	deleteContainerStmt   *sql.Stmt
	getContainersStmt     *sql.Stmt
}

func (q *Queries) WithTx(tx *sql.Tx) *Queries {
	return &Queries{
		db:                    tx,
		tx:                    tx,
		addContainerStmt:      q.addContainerStmt,
		addContainerAddrStmt:  q.addContainerAddrStmt,
		addContainerAliasStmt: q.addContainerAliasStmt,
		deleteContainerStmt:   q.deleteContainerStmt,
		getContainersStmt:     q.getContainersStmt,
	}
}

const addContainer = "INSERT INTO containers(id, name) VALUES (?, ?)"

func (q *Queries) AddContainer(ctx context.Context, iD string, name string) error {
	_, err := q.exec(ctx, q.addContainerStmt, addContainer, iD, name)
	return err
}

const addContainerAddr = "INSERT INTO addrs(addr, container_id) VALUES (?, ?)"

func (q *Queries) AddContainerAddr(ctx context.Context, addr []byte, containerID string) error {
	_, err := q.exec(ctx, q.addContainerAddrStmt, addContainerAddr, addr, containerID)
	return err
}

const addContainerAlias = "INSERT INTO container_aliases(container_id, container_alias) VALUES (?, ?)"

func (q *Queries) AddContainerAlias(ctx context.Context, containerID string, containerAlias string) error {
	_, err := q.exec(ctx, q.addContainerAliasStmt, addContainerAlias, containerID, containerAlias)
	return err
}

const deleteContainer = "DELETE FROM containers WHERE id = ?"

func (q *Queries) DeleteContainer(ctx context.Context, id string) error {
	_, err := q.exec(ctx, q.deleteContainerStmt, deleteContainer, id)
	return err
}

const getContainers = "SELECT id, name FROM containers"

func (q *Queries) GetContainers(ctx context.Context) error {
	rows, err := q.query(ctx, q.getContainersStmt, getContainers)
	if err != nil {
		return err
	}
	return rows.Close()
}

type db struct {
	*Queries
	db *sql.DB
}

func NewDB(ctx context.Context, database *sql.DB) (*db, error) {
	q, err := Prepare(ctx, database)
	if err != nil {
		return nil, err
	}

	return &db{
		Queries: q,
		db:      database,
	}, nil
}

type TX interface {
	*Queries
	Rollback() bool
	Commit() error
}

type tx struct {
	*Queries
	ctx context.Context
}

func (d *db) Begin(ctx context.Context) (*tx, error) {
	transaction, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("error beginning database transaction: %w", err)
	}

	return &tx{
		Queries: d.WithTx(transaction),
		ctx:     ctx,
	}, nil
}

func (t *tx) Rollback() bool {
	if err := t.tx.Rollback(); err != nil {
		if errors.Is(err, sql.ErrTxDone) {
			return false
		}
		log.Printf("error rolling back database transaction: %v", err)
		return false
	}

	return true
}

func (t *tx) Commit() error {
	if err := t.tx.Commit(); err != nil {
		if errors.Is(err, sql.ErrTxDone) && t.ctx.Err() != nil {
			err = t.ctx.Err()
		}
		return fmt.Errorf("error committing database transaction: %w", err)
	}

	return nil
}
