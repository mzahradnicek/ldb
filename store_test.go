package ldb

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	sqlg "github.com/mzahradnicek/sql-glue/v2"
)

var conn *Connection

type testUser struct {
	Store

	Id       int
	Name     string
	StatusId int
}

func (o *testUser) Find() error {
	q := &sqlg.Qg{"SELECT * FROM users WHERE id = 1"}

	// Create a temporary struct for scanning to avoid losing the Store connection
	var temp struct {
		Id       int
		Name     string
		StatusId int
	}

	if err := o.Conn().GlueGet(context.Background(), q, &temp); err != nil {
		return err
	}

	// Copy the values to the model
	o.Id = temp.Id
	o.Name = temp.Name
	o.StatusId = temp.StatusId

	// Second query to test that connection is preserved
	if err := o.Conn().GlueGet(context.Background(), q, &temp); err != nil {
		return err
	}

	o.Id = temp.Id
	o.Name = temp.Name
	o.StatusId = temp.StatusId

	return nil
}

func init() {
	SetScannerMapper(sqlg.ToCamel)

	sqlBuilder := sqlg.NewBuilder(&sqlg.Config{
		KeyModifier:      sqlg.ToSnake,
		IdentifierEscape: func(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` },
		PlaceholderInit:  sqlg.QmPlaceholderInit,
		Tag:              "sqlg",
	})

	if err := NewConnection("default", "test.db", sqlBuilder); err != nil {
		fmt.Println("Can't establish connection to test DB")
		os.Exit(1)
	}
}

func TestStore(t *testing.T) {
	user := newtestUser(GetConnection())

	if err := user.Find(); err != nil {
		t.Errorf("Glue get error: %v", err)
	}
}

func newtestUser(c *Connection) *testUser {
	return &testUser{Store: *NewStore(c)}
}
