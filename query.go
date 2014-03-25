package database

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/simonz05/util/log"
)

type Args map[string]interface{}

func (a *Args) Has(k string) bool {
	_, ok := (*a)[k]
	return ok
}

func (a *Args) IsTrue(k string) bool {
	v, ok := (*a)[k]

	if !ok {
		return false
	}

	b, ok := v.(bool)

	if !ok {
		return false
	}

	return b
}

func GetFilter(filters []map[string]interface{}) (filter Args) {
	if len(filters) > 0 {
		filter = filters[0]
	}

	return
}

func PrepareWhere(w []string) string {
	if len(w) == 0 {
		return ""
	}

	return " WHERE " + strings.Join(w, " AND ")
}

func PrepareIN(n int) string {
	return strings.Repeat("?, ", n-1) + "?"
}

func PrepareIntIN(args *[]interface{}, w *[]string, value interface{}, table, field string) error {
	switch v := value.(type) {
	case []int:
		if len(v) == 0 {
			break
		}
		inPart := PrepareIN(len(v))
		*w = append(*w, fmt.Sprintf("%s.%s IN (%s)", table, field, inPart))
		*args = append(*args, intToIface(v)...)
	case int:
		*args = append(*args, v)
		*w = append(*w, fmt.Sprintf("%s.%s = ?", table, field))
	default:
		err := fmt.Errorf("expected %s type int or []int, got %T", field, v)
		log.Error(err.Error())
		return err
	}
	return nil
}

func Scalar(conn Conn, query string, args ...interface{}) (int64, error) {
	row, err := QueryRowx(conn, query, args...)

	if err != nil {
		return 0, err
	}

	var value int64
	err = row.Scan(&value)
	return value, err
}

func Prepare(conn Conn, query string) (*sqlx.Stmt, error) {
	switch c := conn.(type) {
	case *sqlx.Tx:
		return c.Preparex(query)
	case *DB:
		return c.stmtCache.get(conn, query)
	default:
		return c.Preparex(query)
	}
}

func StmtClose(conn Conn, stmt *sqlx.Stmt) error {
	if stmt == nil {
		return nil
	}

	switch conn.(type) {
	case *DB:
		return nil
	default:
		return stmt.Close()
	}
}

func Exec(conn Conn, query string, args ...interface{}) (sql.Result, error) {
	return conn.Exec(query, args...)
	// if stmt, err := Prepare(conn, query); err != nil {
	// 	return nil, err
	// } else {
	// 	defer StmtClose(conn, stmt)
	// 	return stmt.Exec(args...)
	// }
}

func Queryx(conn Conn, query string, args ...interface{}) (*sqlx.Rows, error) {
	return conn.Queryx(query, args...)
	//if stmt, err := Prepare(conn, query); err != nil {
	//	return nil, err
	//} else {
	//	defer StmtClose(conn, stmt)
	//	return stmt.Queryx(args...)
	//}
}

func QueryRowx(conn Conn, query string, args ...interface{}) (*sqlx.Row, error) {
	return conn.QueryRowx(query, args...), nil
	//stmt, err := Prepare(conn, query)
	//defer StmtClose(conn, stmt)

	//if err != nil {
	//	fmt.Println("ERR", err)
	//	return nil, err
	//} else {
	//	return stmt.QueryRowx(args...), nil
	//}
}

func Select(exec Conn, dest interface{}, query string, args ...interface{}) error {
	return querySelect(DefaultDBMap, exec, dest, query, args...)
}

func Put(exec Conn, isNew bool, list ...interface{}) error {
	if isNew {
		return Insert(exec, list...)
	}
	_, err := Update(exec, list...)
	return err
}

func Update(exec Conn, list ...interface{}) (int64, error) {
	return queryUpdate(DefaultDBMap, exec, list...)
}

func Insert(exec Conn, list ...interface{}) error {
	return queryInsert(DefaultDBMap, exec, list...)
}

func Delete(exec Conn, list ...interface{}) (int64, error) {
	return queryDelete(DefaultDBMap, exec, list...)
}

func querySelect(m *DbMap, exec Conn, dest interface{}, query string, args ...interface{}) error {
	t := reflect.TypeOf(dest)

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	} else {
		return fmt.Errorf("select dest must be a pointer, but got: %t", dest)
	}

	//stmt, err := Prepare(exec, query)
	//defer StmtClose(exec, stmt)

	//if err != nil {
	//	return err
	//}

	switch t.Kind() {
	case reflect.Struct:
		//row := stmt.QueryRowx(args...)
		row := exec.QueryRowx(query, args...)
		return row.StructScan(dest)
	case reflect.Slice:
		//sqlrows, err := stmt.Query(args...)
		sqlrows, err := exec.Query(query, args...)
		defer sqlrows.Close()
		if err != nil {
			return err
		}
		return sqlx.StructScan(sqlrows, dest)
	default:
		return fmt.Errorf("select dest must be a pointer to a slice or struct, but got: %t", dest)
	}
}

func queryDelete(m *DbMap, exec Conn, list ...interface{}) (int64, error) {
	var err error
	var table *TableMap
	var elem reflect.Value
	var count int64

	for _, ptr := range list {
		table, elem, err = tableForPointer(m, ptr, true)
		if err != nil {
			return -1, err
		}

		bi := table.bindDelete(elem)
		//stmt, err := Prepare(exec, bi.query)
		//defer StmtClose(exec, stmt)
		//
		//f err != nil {
		//	return -1, err
		//}

		//res, err := stmt.Exec(bi.args...)
		res, err := exec.Exec(bi.query, bi.args...)

		if err != nil {
			return -1, err
		}

		rows, err := res.RowsAffected()
		if err != nil {
			return -1, err
		}

		count += rows
	}

	return count, nil
}

func queryUpdate(m *DbMap, exec Conn, list ...interface{}) (int64, error) {
	var err error
	var table *TableMap
	var elem reflect.Value
	var count int64

	for _, ptr := range list {
		table, elem, err = tableForPointer(m, ptr, true)
		if err != nil {
			return -1, err
		}

		bi := table.bindUpdate(elem)
		if err != nil {
			return -1, err
		}

		// stmt, err := Prepare(exec, bi.query)
		// defer StmtClose(exec, stmt)

		// if err != nil {
		// 	return -1, err
		// }

		// res, err := stmt.Exec(bi.args...)
		res, err := exec.Exec(bi.query, bi.args...)

		if err != nil {
			return -1, err
		}

		rows, err := res.RowsAffected()
		if err != nil {
			return -1, err
		}

		count += rows
	}
	return count, nil
}

func queryInsert(m *DbMap, exec Conn, list ...interface{}) error {
	var err error
	var table *TableMap
	var elem reflect.Value

	for _, ptr := range list {
		table, elem, err = tableForPointer(m, ptr, false)
		if err != nil {
			return err
		}

		bi := table.bindInsert(elem)
		//stmt, err := Prepare(exec, bi.query)
		//defer StmtClose(exec, stmt)

		//if err != nil {
		//	return err
		//}

		if bi.autoIncrIdx > -1 {
			//res, err := stmt.Exec(bi.args...)
			res, err := exec.Exec(bi.query, bi.args...)

			if err != nil {
				return err
			}

			id, err := res.LastInsertId()

			if err != nil {
				return err
			}

			f := elem.Field(bi.autoIncrIdx)
			k := f.Kind()

			if (k == reflect.Int) || (k == reflect.Int16) || (k == reflect.Int32) || (k == reflect.Int64) {
				f.SetInt(id)
			} else {
				return fmt.Errorf("Cannot set autoincrement value on non-Int field. SQL=%s  autoIncrIdx=%d", bi.query, bi.autoIncrIdx)
			}
		} else {
			//_, err := stmt.Exec(bi.args...)
			_, err := exec.Exec(bi.query, bi.args...)

			if err != nil {
				return err
			}
		}
	}
	return nil
}
