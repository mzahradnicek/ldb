package ldb

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrTypeAssertionArrayByte          = errors.New("Type assertion .([]byte) failed.")
	ErrTypeAssertionMapStringInterface = errors.New("Type assertion .(map[string]interface{}) failed.")
)

type NullBool sql.NullBool

func (nb NullBool) MarshalJSON() ([]byte, error) {
	if !nb.Valid {
		return []byte("null"), nil
	}

	return json.Marshal(nb.Bool)
}

func (nb *NullBool) UnmarshalJSON(b []byte) error {
	if strings.ToLower(string(b)) == "null" {
		nb.Valid = false
		return nil
	}

	err := json.Unmarshal(b, &nb.Bool)
	nb.Valid = (err == nil)
	return err
}

type NullFloat64 sql.NullFloat64

func (nf NullFloat64) MarshalJSON() ([]byte, error) {
	if !nf.Valid {
		return []byte("null"), nil
	}

	return json.Marshal(nf.Float64)
}

func (nf *NullFloat64) UnmarshalJSON(b []byte) error {
	if strings.ToLower(string(b)) == "null" {
		nf.Valid = false
		return nil
	}

	err := json.Unmarshal(b, &nf.Float64)
	nf.Valid = (err == nil)
	return err
}

type NullInt64 sql.NullInt64

func (ni NullInt64) MarshalJSON() ([]byte, error) {
	if !ni.Valid {
		return []byte("null"), nil
	}

	return json.Marshal(ni.Int64)
}

func (ni *NullInt64) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), "\"")
	if strings.ToLower(s) == "null" {
		ni.Valid = false
		return nil
	}

	err := json.Unmarshal([]byte(s), &ni.Int64)
	ni.Valid = (err == nil)
	return err
}

type NullString sql.NullString

func (ns NullString) MarshalJSON() ([]byte, error) {
	if !ns.Valid {
		return []byte("null"), nil
	}

	return json.Marshal(ns.String)
}

func (ns *NullString) UnmarshalJSON(b []byte) error {
	if strings.ToLower(string(b)) == "null" {
		ns.Valid = false
		return nil
	}

	err := json.Unmarshal(b, &ns.String)
	ns.Valid = (err == nil)
	return err
}

type NullTime sql.NullTime

func (nt NullTime) MarshalJSON() ([]byte, error) {
	if !nt.Valid {
		return []byte("null"), nil
	}

	return json.Marshal(nt.Time)
}

func (nt *NullTime) UnmarshalJSON(b []byte) error {
	if strings.ToLower(string(b)) == "null" {
		nt.Valid = false
		return nil
	}

	err := json.Unmarshal(b, &nt.Time)
	nt.Valid = (err == nil)
	return err
}

type Json map[string]interface{}

func (o Json) Value() (driver.Value, error) {
	if o == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(o)
}

func (o *Json) Scan(value interface{}) error {
	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	case nil:
		*o = make(map[string]interface{})
		return nil
	default:
		return fmt.Errorf("unsupported type for Json: %T", value)
	}

	return json.Unmarshal(bytes, o)
}

type JsonRaw json.RawMessage

func (j *JsonRaw) Scan(value interface{}) error {
	switch v := value.(type) {
	case []byte:
		*j = append((*j)[:0], v...)
		return nil
	case string:
		*j = []byte(v)
		return nil
	case nil:
		*j = nil
		return nil
	default:
		return fmt.Errorf("unsupported type for JSONRaw: %T", value)
	}
}

func (j JsonRaw) Value() (driver.Value, error) {
	if len(j) == 0 {
		return nil, nil
	}
	return []byte(j), nil
}
