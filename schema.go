package database

import (
	"io/ioutil"
	"path"
	"runtime"
	"strings"

	"github.com/jmoiron/sqlx"
)

func MultiExecFromFile(e sqlx.Execer, filename string) error {
	_, thisFilename, _, _ := runtime.Caller(1)
	absfilepath := path.Join(path.Dir(thisFilename), filename)
	buf, err := ioutil.ReadFile(absfilepath)

	if err != nil {
		return err
	}

	return MultiExec(e, string(buf))
}

func MultiExec(e sqlx.Execer, query string) error {
	stmts := strings.Split(query, ";\n")

	if len(strings.Trim(stmts[len(stmts)-1], " \n\t\r")) == 0 {
		stmts = stmts[:len(stmts)-1]
	}

	for _, s := range stmts {
		if _, err := e.Exec(s); err != nil {
			return err
		}
	}
	return nil
}
