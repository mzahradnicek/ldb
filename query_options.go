package ldb

import (
	"errors"
	"net/url"
	"slices"
	"strconv"
	"strings"

	sqlg "github.com/mzahradnicek/sql-glue/v2"
)

/* Usage example
func CommonQueryOptions(sd string, sa []string, pa []string) *QueryOptions {
	return NewQueryOptions(&QueryOptionsConfig{
		SortAllow: sa,
		SortDefault: sd,
		ParamsAllow: pa,
		LimitMax: 300,
		LimitDefault: 100,
	})
}
*/

var (
	ErrNoLimitDefined = errors.New("No limit defined")
)

type QueryOptionsConfig struct {
	SortAllow    []string
	SortDefault  string
	LimitMax     int
	LimitDefault int
	ParamsAllow  []string
}

type QueryOptions struct {
	Limit  int
	Offset int
	Page   int
	Sort   []string

	cfg    *QueryOptionsConfig
	params map[string]interface{}
}

func (o *QueryOptions) FillFromMap(m map[string]string) error {
	for k, v := range m {
		if err := o.processInput(k, v); err != nil {
			return err
		}
	}

	return nil
}

func (o *QueryOptions) FillFromUrl(m url.Values) (err error) {
	for k, v := range m {
		if err = o.processInput(k, v[0]); err != nil {
			return err
		}
	}

	return nil
}

func (o *QueryOptions) processInput(name, value string) (err error) {
	switch {
	case name == "limit":
		o.Limit, _ = strconv.Atoi(value)
		if o.Limit == 0 {
			o.Limit = o.cfg.LimitDefault
		} else if o.Limit > o.cfg.LimitMax {
			o.Limit = o.cfg.LimitMax
		}
	case name == "offset":
		o.Offset, err = strconv.Atoi(value)
	case name == "page":
		o.Page, err = strconv.Atoi(value)
	case name == "sort":
		s := strings.Split(value, ":")
		if slices.Contains(o.cfg.SortAllow, s[0]) {
			break
		}

		if len(s) == 2 && s[1] == "desc" {
			s[0] += " DESC"
		}

		o.Sort = append(o.Sort, s[0])
	default:
		if slices.Contains(o.cfg.ParamsAllow, name) {
			o.params[name] = value
		}
	}

	return err
}

func (o *QueryOptions) ApplyToQuery(sb *sqlg.Qg) error {
	if o.Page > 0 {
		if o.Limit == 0 {
			return ErrNoLimitDefined
		}

		o.Offset = o.Limit * (o.Page - 1)
	}

	if len(o.Sort) > 0 {
		sb.Append("ORDER BY", strings.Join(o.Sort, ", "))
	} else if o.cfg.SortDefault != "" {
		sb.Append("ORDER BY", o.cfg.SortDefault)
	}

	if o.Limit > 0 {
		sb.Append("LIMIT %v", o.Limit)
	}

	if o.Offset > 0 {
		sb.Append("OFFSET %v", o.Offset)
	}

	return nil
}

/* Params */
func (o *QueryOptions) SetParam(name, value string) *QueryOptions {
	o.params[name] = value
	return o
}

func (o *QueryOptions) GetParam(name string) interface{} {
	val, _ := o.params[name]
	return val
}

func (o *QueryOptions) HasParam(name string) bool {
	_, ok := o.params[name]
	return ok
}

func NewQueryOptions(cfg *QueryOptionsConfig) *QueryOptions {
	return &QueryOptions{
		Limit: cfg.LimitDefault,

		cfg:    cfg,
		params: make(map[string]interface{}),
	}
}
