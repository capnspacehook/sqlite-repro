package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
)

type DB interface {
	Querier
	Begin(ctx context.Context) (TX, error)
	io.Closer
}

type db struct {
	*Queries
	db *sql.DB
}

func NewDB(ctx context.Context, database *sql.DB) (DB, error) {
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
	Querier
	Rollback() bool
	Commit() error
}

type tx struct {
	*Queries
	ctx context.Context
}

func (d *db) Begin(ctx context.Context) (TX, error) {
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
