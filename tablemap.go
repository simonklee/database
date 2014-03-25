package database

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/jmoiron/sqlx"
)

type NoKeysErr struct {
	Table *TableMap
}

func (n NoKeysErr) Error() string {
	return fmt.Sprintf("Could not find keys for table %v", n.Table)
}

// TableMap represents a mapping between a Go struct and a database table
// Use dbmap.AddTable() or dbmap.AddTableWithName() to create these
type TableMap struct {
	// Name of database table.
	TableName  string
	gotype     reflect.Type
	columns    []*ColumnMap
	columnsStr string
	keys       []*ColumnMap
	insertPlan bindPlan
	updatePlan bindPlan
	deletePlan bindPlan
	getPlan    bindPlan
	dbmap      *DbMap
}

// ResetSql removes cached insert/update/select/delete SQL strings
// associated with this TableMap.  Call this if you've modified
// any column names or the table name itself.
func (t *TableMap) ResetSql() {
	t.insertPlan = bindPlan{}
	t.updatePlan = bindPlan{}
	t.deletePlan = bindPlan{}
	t.getPlan = bindPlan{}
}

// SetKeys lets you specify the fields on a struct that map to primary
// key columns on the table.  If isAutoIncr is set, result.LastInsertId()
// will be used after INSERT to bind the generated id to the Go struct.
//
// Automatically calls ResetSql() to ensure SQL statements are regenerated.
func (t *TableMap) SetKeys(isAutoIncr bool, fieldNames ...string) *TableMap {
	t.keys = make([]*ColumnMap, 0)
	for _, name := range fieldNames {
		colmap := t.ColMap(sqlx.NameMapper(name))
		colmap.isPK = true
		colmap.isAutoIncr = isAutoIncr
		t.keys = append(t.keys, colmap)
	}
	t.ResetSql()

	return t
}

// ColMap returns the ColumnMap pointer matching the given struct field
// name.  It panics if the struct does not contain a field matching this
// name.
func (t *TableMap) ColMap(field string) *ColumnMap {
	col := colMapOrNil(t, field)
	if col == nil {
		e := fmt.Sprintf("No ColumnMap in table %s type %s with field %s",
			t.TableName, t.gotype.Name(), field)

		panic(e)
	}
	return col
}

// Return the column map for this field, or nil if it can't be found.
func colMapOrNil(t *TableMap, field string) *ColumnMap {
	for _, col := range t.columns {
		if col.fieldName == field || col.ColumnName == field {
			return col
		}
	}
	return nil
}

func (t *TableMap) ColumnsStr() string {
	t.setColumnsStr()
	return t.columnsStr
}

func (t *TableMap) setColumnsStr() {
	if t.columnsStr == "" {
		s := bytes.Buffer{}
		x := 0
		for _, col := range t.columns {
			if !col.Transient {
				if x > 0 {
					s.WriteString(",")
				}
				s.WriteString(QuoteField(t.TableName))
				s.WriteString(".")
				s.WriteString(QuoteField(col.ColumnName))
				x++
			}
		}
		t.columnsStr = s.String()
	}
}

func (t *TableMap) bindGet() bindPlan {
	plan := t.getPlan
	if plan.query == "" {

		s := bytes.Buffer{}
		s.WriteString("select ")

		x := 0
		for _, col := range t.columns {
			if !col.Transient {
				if x > 0 {
					s.WriteString(",")
				}
				s.WriteString(QuoteField(col.ColumnName))
				plan.argFields = append(plan.argFields, col.fieldName)
				x++
			}
		}
		s.WriteString(" FROM ")
		s.WriteString(QuoteField(t.TableName))
		s.WriteString(" WHERE ")
		for x := range t.keys {
			col := t.keys[x]
			if x > 0 {
				s.WriteString(" AND ")
			}
			s.WriteString(QuoteField(col.ColumnName))
			s.WriteString("=")
			s.WriteString("?")

			plan.keyFields = append(plan.keyFields, col.fieldName)
		}
		s.WriteString(";")

		plan.query = s.String()
		t.getPlan = plan
	}

	return plan
}

func (t *TableMap) bindDelete(elem reflect.Value) bindInstance {
	plan := t.deletePlan
	if plan.query == "" {

		s := bytes.Buffer{}
		s.WriteString(fmt.Sprintf("DELETE FROM %s", QuoteField(t.TableName)))
		s.WriteString(" WHERE ")

		for x := range t.keys {
			k := t.keys[x]
			if x > 0 {
				s.WriteString(" AND ")
			}
			s.WriteString(QuoteField(k.ColumnName))
			s.WriteString("=")
			s.WriteString("?")

			plan.keyFields = append(plan.keyFields, k.fieldName)
			plan.argFields = append(plan.argFields, k.fieldName)
		}
		s.WriteString(";")

		plan.query = s.String()
		t.deletePlan = plan
	}

	return plan.createBindInstance(elem)
}

func (t *TableMap) bindUpdate(elem reflect.Value) bindInstance {
	plan := t.updatePlan
	if plan.query == "" {

		s := bytes.Buffer{}
		s.WriteString(fmt.Sprintf("UPDATE %s SET ", QuoteField(t.TableName)))
		x := 0

		for y := range t.columns {
			col := t.columns[y]
			if !col.isPK && !col.Transient {
				if x > 0 {
					s.WriteString(", ")
				}
				s.WriteString(QuoteField(col.ColumnName))
				s.WriteString("=")
				s.WriteString("?")

				plan.argFields = append(plan.argFields, col.fieldName)
				x++
			}
		}

		s.WriteString(" WHERE ")
		for y := range t.keys {
			col := t.keys[y]
			if y > 0 {
				s.WriteString(" AND ")
			}
			s.WriteString(QuoteField(col.ColumnName))
			s.WriteString("=")
			s.WriteString("?")

			plan.argFields = append(plan.argFields, col.fieldName)
			plan.keyFields = append(plan.keyFields, col.fieldName)
			x++
		}
		s.WriteString(";")

		plan.query = s.String()
		t.updatePlan = plan
	}

	return plan.createBindInstance(elem)
}

func (t *TableMap) bindInsert(elem reflect.Value) bindInstance {
	plan := t.insertPlan
	if plan.query == "" {
		plan.autoIncrIdx = -1

		s := bytes.Buffer{}
		s2 := bytes.Buffer{}
		s.WriteString(fmt.Sprintf("INSERT INTO %s (", QuoteField(t.TableName)))

		x := 0
		first := true
		for y := range t.columns {
			col := t.columns[y]

			if !col.Transient {
				if !first {
					s.WriteString(",")
					s2.WriteString(",")
				}
				s.WriteString(QuoteField(col.ColumnName))

				if col.isAutoIncr {
					s2.WriteString("NULL")
					plan.autoIncrIdx = y
				} else {
					s2.WriteString("?")
					plan.argFields = append(plan.argFields, col.fieldName)

					x++
				}

				first = false
			}
		}
		s.WriteString(") VALUES (")
		s.WriteString(s2.String())
		s.WriteString(")")
		if plan.autoIncrIdx > -1 {
			s.WriteString("")
		}
		s.WriteString(";")

		plan.query = s.String()
		t.insertPlan = plan
	}

	return plan.createBindInstance(elem)
}

// ColumnMap represents a mapping between a Go struct field and a single
// column in a table.
// Unique and MaxSize only inform the CreateTables() function and are not
// used for validation by Insert/Update/Delete/Get.
type ColumnMap struct {
	// Column name in db table
	ColumnName string

	// If true, this column is skipped in generated SQL statements
	Transient bool

	// If true, " unique" is added to create table statements.
	Unique bool

	// Passed to Dialect.ToSqlType() to assist in informing the
	// correct column type to map to in CreateTables()
	MaxSize int

	fieldName  string
	gotype     reflect.Type
	sqltype    string
	isPK       bool
	isAutoIncr bool
}

// SetTransient allows you to mark the column as transient. If true
// this column will be skipped when SQL statements are generated
func (c *ColumnMap) SetTransient(b bool) *ColumnMap {
	c.Transient = b
	return c
}

type bindPlan struct {
	query       string
	argFields   []string
	keyFields   []string
	autoIncrIdx int
}

func (plan bindPlan) createBindInstance(elem reflect.Value) bindInstance {
	bi := bindInstance{query: plan.query, autoIncrIdx: plan.autoIncrIdx}

	for i := 0; i < len(plan.argFields); i++ {
		k := plan.argFields[i]
		val := elem.FieldByName(k).Interface()
		bi.args = append(bi.args, val)
	}

	for i := 0; i < len(plan.keyFields); i++ {
		k := plan.keyFields[i]
		val := elem.FieldByName(k).Interface()
		bi.keys = append(bi.keys, val)
	}

	return bi
}

type bindInstance struct {
	query       string
	args        []interface{}
	keys        []interface{}
	autoIncrIdx int
}
