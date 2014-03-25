package database

import (
	"fmt"
	"reflect"

	"github.com/jmoiron/sqlx"
)

// Return a table for a pointer;  error if i is not a pointer or if the
// table is not found
func tableForPointer(m *DbMap, i interface{}, checkPk bool) (*TableMap, reflect.Value, error) {
	v := reflect.ValueOf(i)
	if v.Kind() != reflect.Ptr {
		return nil, v, fmt.Errorf("Value %v not a pointer", v)
	}
	v = v.Elem()
	t := m.TableForType(v.Type())
	if t == nil {
		return nil, v, fmt.Errorf("Could not find table for %v", t)
	}
	if checkPk && len(t.keys) < 1 {
		return t, v, &NoKeysErr{t}
	}
	return t, v, nil
}

type DbMap struct {
	tables []*TableMap
}

// AddTable registers the given interface type with modl. The table name
// will be given the name of the TypeOf(i), lowercased.
//
// This operation is idempotent. If i's type is already mapped, the
// existing *TableMap is returned
func (m *DbMap) AddTable(i interface{}, name ...string) *TableMap {
	Name := ""
	if len(name) > 0 {
		Name = name[0]
	}

	t := reflect.TypeOf(i)
	if len(Name) == 0 {
		Name = sqlx.NameMapper(t.Name())
	}

	// check if we have a table for this type already
	// if so, update the name and return the existing pointer
	for i := range m.tables {
		table := m.tables[i]
		if table.gotype == t {
			table.TableName = Name
			return table
		}
	}

	tmap := &TableMap{gotype: t, TableName: Name, dbmap: m}

	n := t.NumField()
	tmap.columns = make([]*ColumnMap, 0, n)
	for i := 0; i < n; i++ {
		f := t.Field(i)
		columnName := f.Tag.Get("db")
		if columnName == "" {
			columnName = sqlx.NameMapper(f.Name)
		}

		cm := &ColumnMap{
			ColumnName: columnName,
			Transient:  columnName == "-",
			fieldName:  f.Name,
			gotype:     f.Type,
		}
		tmap.columns = append(tmap.columns, cm)
	}
	m.tables = append(m.tables, tmap)
	return tmap
}

func (m *DbMap) AddTableWithName(i interface{}, name string) *TableMap {
	return m.AddTable(i, name)
}

// Returns any matching tables for the interface i or nil if not found
// If i is a slice, then the table is given for the base slice type
func (m *DbMap) TableFor(i interface{}) *TableMap {
	var t reflect.Type
	v := reflect.ValueOf(i)
start:
	switch v.Kind() {
	case reflect.Ptr:
		// dereference pointer and try again;  we never want to store pointer
		// types anywhere, that way we always know how to do lookups
		v = v.Elem()
		goto start
	case reflect.Slice:
		// if this is a slice of X's, we're interested in the type of X
		t = v.Type().Elem()
	default:
		t = v.Type()
	}
	return m.TableForType(t)
}

// Returns any matching tables for the type t or nil if not found
func (m *DbMap) TableForType(t reflect.Type) *TableMap {
	for _, table := range m.tables {
		if table.gotype == t {
			return table
		}
	}
	return nil
}

var DefaultDBMap *DbMap

func init() {
	DefaultDBMap = &DbMap{}
}
