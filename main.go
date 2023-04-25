package main

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
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

	err = tx.AddContainer(context.Background(), AddContainerParams{
		ID:   "dummyid",
		Name: "dummyname",
	})
	handleErr(err)
	err = tx.DeleteContainer(context.Background(), "dummyid")
	handleErr(err)

	handleErr(tx.Commit())
	tx.Rollback()

	ctx, cancel := context.WithCancel(context.Background())
	tx, err = db.Begin(ctx)
	handleErr(err)

	err = tx.AddContainer(ctx, AddContainerParams{
		ID:   cont1ID,
		Name: cont1Name,
	})
	handleErr(err)
	err = tx.AddContainerAddr(ctx, AddContainerAddrParams{
		Addr:        cont1Addr.AsSlice(),
		ContainerID: cont1ID,
	})
	handleErr(err)
	err = tx.AddContainerAlias(ctx, AddContainerAliasParams{
		ContainerID:    cont1ID,
		ContainerAlias: "/" + cont1Name,
	})
	handleErr(err)

	cancel()
	err = tx.Commit()
	if !errors.Is(err, context.Canceled) {
		handleErr(err)
	}
	tx.Rollback()

	conts, err := db.GetContainers(context.Background())
	handleErr(err)
	fmt.Println(conts)
}
