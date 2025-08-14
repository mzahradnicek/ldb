package ldb

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"slices"
	"time"

	"github.com/blockloop/scan/v2"
	_ "github.com/mattn/go-sqlite3"
	. "github.com/mzahradnicek/slog-helpers"
	sqlg "github.com/mzahradnicek/sql-glue/v2"
)

type Gopt int

const (
	OPT_LOG Gopt = iota
)

var (
	ErrNoBegin    = errors.New("Can't call Begin on connection")
	ErrNoRollback = errors.New("Can't call Rollback on connection")
	ErrNoCommit   = errors.New("Can't call Commit on connection")
)

type Connection struct {
	db   Querier
	sqlg *sqlg.Builder

	lastUsed time.Time
}

func (c *Connection) GetDB() *sql.DB {
	v, ok := c.db.(*sql.DB)

	if ok {
		return v
	}

	return nil
}

func (c *Connection) GlueExec(ctx context.Context, q *sqlg.Qg, opt ...Gopt) (sql.Result, error) {
	c.lastUsed = time.Now()
	sql, args, err := c.sqlg.Glue(q)

	if err != nil {
		return nil, NewError(err, "query", *q)
	}

	ct, err := c.db.ExecContext(ctx, sql, args...)
	if err != nil {
		err = NewError(err, "sql", sql, "args", args)
	}

	if slices.Contains(opt, OPT_LOG) {
		slog.Debug("GlueExec Query", "slq", sql, "args", args)
	}

	return ct, err
}
func (c *Connection) GlueQueryRowScan(ctx context.Context, q *sqlg.Qg, dst []interface{}, opt ...Gopt) error {
	c.lastUsed = time.Now()
	sql, args, err := c.sqlg.Glue(q)

	if err != nil {
		return NewError(err, "query", *q)
	}

	err = c.db.QueryRowContext(ctx, sql, args...).Scan(dst...)
	if err != nil {
		return NewError(err, "sql", sql, "args", args)
	}

	if slices.Contains(opt, OPT_LOG) {
		slog.Debug("GlueQueryRowScan Query", "slq", sql, "args", args)
	}

	return nil
}

func (c *Connection) GlueSelect(ctx context.Context, q *sqlg.Qg, dst interface{}, opt ...Gopt) error {
	c.lastUsed = time.Now()
	sql, args, err := c.sqlg.Glue(q)

	if err != nil {
		return NewError(err, "query", *q)
	}

	rows, err := c.db.QueryContext(ctx, sql, args...)
	if err != nil {
		return NewError(err, "sql", sql, "args", args)
	}

	err = scan.Rows(dst, rows)
	if err != nil {
		return NewError(err, "sql", sql, "args", args)
	}

	if slices.Contains(opt, OPT_LOG) {
		slog.Debug("GlueQueryRowScan Query", "slq", sql, "args", args)
	}

	return nil
}

func (c *Connection) GlueGet(ctx context.Context, q *sqlg.Qg, dst interface{}, opt ...Gopt) error {
	c.lastUsed = time.Now()
	sql, args, err := c.sqlg.Glue(q)

	if err != nil {
		return NewError(err, "query", *q)
	}

	rows, err := c.db.QueryContext(ctx, sql, args...)
	if err != nil {
		return NewError(err, "sql", sql, "args", args)
	}

	err = scan.Row(dst, rows)
	if err != nil {
		return NewError(err, "sql", sql, "args", args)
	}

	if slices.Contains(opt, OPT_LOG) {
		slog.Debug("GlueQueryRowScan Query", "slq", sql, "args", args)
	}

	return nil
}

/* Transaction helpers */
func (c *Connection) Begin(ctx context.Context) (*Connection, error) {
	if v, ok := c.db.(TxBeginer); ok {
		tx, err := v.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}

		return &Connection{db: tx, sqlg: c.sqlg}, nil
	}

	return nil, ErrNoBegin
}

func (c *Connection) Rollback(ctx context.Context) error {
	if v, ok := c.db.(*sql.Tx); ok {
		return v.Rollback()
	}

	return ErrNoRollback
}

func (c *Connection) Commit(ctx context.Context) error {
	if v, ok := c.db.(*sql.Tx); ok {
		return v.Commit()
	}

	return ErrNoCommit
}
