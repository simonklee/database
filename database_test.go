package database

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/simonz05/util/assert"
)

var regOnce sync.Once
var dbMap *DbMap

func registerTables() {
	dbMap = DefaultDBMap
	dbMap.AddTableWithName(Friend{}, "Friend").SetKeys(true, "FriendID")
}

func init() {
	regOnce.Do(registerTables)
}

type Friend struct {
	FriendID int
	Name     string
}

func TestDatabase(t *testing.T) {
	db := dbFromConf(t)
	setUp(t, db)
	defer tearDown(t, db)
	ast := assert.NewAssert(t)

	cnt, err := friendCount(db)
	ast.Nil(err)
	ast.Equal(0, cnt)

	f := &Friend{Name: "Foo"}
	err = friendPut(db, f)
	ast.Nil(err)
	ast.Equal(1, f.FriendID)
}

func friendCount(conn Conn) (int64, error) {
	q := "SELECT COUNT(*) FROM Friend"
	return Scalar(conn, q)
}

func friendPut(conn Conn, f *Friend) (err error) {
	if f.FriendID == 0 {
		err = Insert(conn, f)
	} else {
		_, err = Update(conn, f)
	}

	return
}

func dbFromConf(t *testing.T) *DB {
	dbname := "testing"
	dsn := fmt.Sprintf("testing:testing@tcp(localhost:3306)/%s?charset=utf8&parseTime=True", dbname)

	if os.Getenv("TRAVIS") == "true" {
		dbname = "myapp_test"
		dsn = fmt.Sprintf("root:@tcp(localhost:3306)/%s?charset=utf8&parseTime=True")
	}

	db := NewDB(dsn)

	if _, err := Exec(db, "CREATE DATABASE IF NOT EXISTS "+dbname); err != nil {
		t.Fatal(err)
	}

	return db
}

func setUp(t *testing.T, conn Conn) {
	createFriend := `CREATE TABLE IF NOT EXISTS Friend (
		FriendID	INT(11)			UNSIGNED NOT NULL AUTO_INCREMENT,
		Name		VARCHAR(255)	NULL DEFAULT '',

		CONSTRAINT Pk_PaymentLog PRIMARY KEY (FriendID)
	) ENGINE=InnoDB CHARSET=utf8 COLLATE=utf8_unicode_ci`

	if _, err := Exec(conn, createFriend); err != nil {
		t.Fatal(err)
	}
}

func tearDown(t *testing.T, conn Conn) {
	if _, err := Exec(conn, "DROP TABLE Friend"); err != nil {
		t.Fatal(err)
	}
}
