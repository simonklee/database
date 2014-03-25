package database

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var ErrNoRows = sql.ErrNoRows

func connect(driver, dsn string) *sql.DB {
	db, err := sql.Open(driver, dsn)

	if err != nil {
		panic("Error connecting to db: " + err.Error())
	}

	return db
}

type DB struct {
	sqlx.DB
	stmtCache *stmtCache
}

func NewDB(dsn string) *DB {
	if dsn == "" {
		dsn = "testing:testing@tcp(localhost:3306)/testing?charset=utf8&parseTime=True"
	}

	sqlxDb := sqlx.NewDb(connect("mysql", dsn), "mysql")
	return &DB{*sqlxDb, newStmtCache()}
}

func (db *DB) LoadSchema(dropTables bool) error {
	return loadSchema(&db.DB, dropTables)
}

type Conn interface {
	sqlx.Queryer
	sqlx.Execer
	sqlx.Preparer
	sqlx.Binder
	Preparex(string) (*sqlx.Stmt, error)
}

var _, _ Conn = &sqlx.DB{}, &sqlx.Tx{}

func init() {
	sqlx.NameMapper = func(v string) string { return v }
}
