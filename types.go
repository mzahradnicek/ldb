package ldb

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
)

var (
	ErrTypeAssertionArrayByte          = errors.New("Type assertion .([]byte) failed.")
	ErrTypeAssertionMapStringInterface = errors.New("Type assertion .(map[string]interface{}) failed.")
)

type NullBool struct {
	sql.NullBool
}

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

type NullFloat64 struct {
	sql.NullFloat64
}

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

type NullInt64 struct {
	sql.NullInt64
}

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

type NullString struct {
	sql.NullString
}

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

type NullTime struct {
	sql.NullTime
}

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

func (b Json) Value() (driver.Value, error) {
	j, err := json.Marshal(b)
	if string(j) == "null" {
		return []byte("{}"), nil
	}
	return j, err
}

func (b *Json) Scan(src interface{}) error {
	source, ok := src.([]byte)
	if !ok {
		return ErrTypeAssertionArrayByte
	}

	var i interface{}
	err := json.Unmarshal(source, &i)
	if err != nil {
		return err
	}

	*b, ok = i.(map[string]interface{})
	if !ok {
		return ErrTypeAssertionMapStringInterface
	}

	return nil
}
