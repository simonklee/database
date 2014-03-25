package database

import (
	"sync"

	"github.com/jmoiron/sqlx"
)

type stmtCache struct {
	cache map[string]*sqlx.Stmt
	mu    sync.RWMutex
}

func newStmtCache() *stmtCache {
	return &stmtCache{
		cache: make(map[string]*sqlx.Stmt),
	}
}

func (s *stmtCache) get(preparer Conn, q string) (*sqlx.Stmt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stmt, ok := s.cache[q]

	if !ok {
		var err error
		stmt, err = preparer.Preparex(q)

		if err != nil {
			return nil, err
		}

		s.cache[q] = stmt
	}

	return stmt, nil
}
