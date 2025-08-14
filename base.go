package ldb

import (
	"context"
	"database/sql"
)

type TxBeginer interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

type Querier interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}
