package sqlwrapper

import (
	"database/sql"
	"log"
	"os"
	"time"
)

type Tx struct {
	log   *log.Logger
	tx    *sql.Tx
	debug bool
}

func (t *Tx) Commit() error {
	st := time.Now()
	defer func() {
		if t.debug {
			et := time.Now()
			t.log.Println("commit", et.Sub(st))
		}
	}()
	return t.tx.Commit()
}
func (t *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	st := time.Now()
	defer func() {
		if t.debug {
			et := time.Now()
			t.log.Println(query, args, et.Sub(st))
		}
	}()
	return t.tx.Exec(query, args...)
}
func (t *Tx) Prepare(query string) (*Stmt, error) {
	s, err := t.tx.Prepare(query)
	if err != nil {
		return nil, err
	}
	stmt := &Stmt{
		log:     t.log,
		stmt:    s,
		debug:   t.debug,
		prepare: query,
	}
	return stmt, nil
}
func (t *Tx) Rollback() error {
	st := time.Now()
	defer func() {
		if t.debug {
			et := time.Now()
			t.log.Println("rollback", et.Sub(st))
		}
	}()
	return t.tx.Rollback()
}
func (t *Tx) Stmt(stmt *Stmt) *Stmt {
	s := t.tx.Stmt(stmt.stmt)
	stmt.stmt = s
	return stmt
}
func (t *Tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	st := time.Now()
	defer func() {
		if t.debug {
			et := time.Now()
			t.log.Println(query, args, et.Sub(st))
		}
	}()
	return t.tx.Query(query, args...)
}
func (t *Tx) QueryRow(query string, args ...interface{}) *sql.Row {
	st := time.Now()
	defer func() {
		if t.debug {
			et := time.Now()
			t.log.Println(query, args, et.Sub(st))
		}
	}()
	return t.tx.QueryRow(query, args...)
}

type Stmt struct {
	log     *log.Logger
	stmt    *sql.Stmt
	prepare string
	debug   bool
}

func (s *Stmt) Exec(args ...interface{}) (sql.Result, error) {
	st := time.Now()
	defer func() {
		if s.debug {
			et := time.Now()
			s.log.Println(s.prepare, args, et.Sub(st))
		}
	}()
	return s.stmt.Exec(args...)
}
func (s *Stmt) Query(args ...interface{}) (*sql.Rows, error) {
	st := time.Now()
	defer func() {
		if s.debug {
			et := time.Now()
			s.log.Println(s.prepare, args, et.Sub(st))
		}
	}()
	return s.stmt.Query(args...)
}
func (s *Stmt) QueryRow(args ...interface{}) *sql.Row {
	st := time.Now()
	defer func() {
		if s.debug {
			et := time.Now()
			s.log.Println(s.prepare, args, et.Sub(st))
		}
	}()
	return s.stmt.QueryRow(args...)
}

type DB struct {
	db    *sql.DB
	log   *log.Logger
	debug bool
}

func WrapperDB(db *sql.DB, debug bool) (d *DB) {
	l := log.New(os.Stdout, "[sql]", log.LstdFlags)

	return &DB{
		db:    db,
		debug: debug,
		log:   l,
	}
}
func (d *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	st := time.Now()
	defer func() {
		if d.debug {
			et := time.Now()
			d.log.Println(query, args, et.Sub(st))
		}
	}()
	return d.db.Exec(query, args...)

}

func (d *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	st := time.Now()
	defer func() {
		if d.debug {
			et := time.Now()
			d.log.Println(query, args, et.Sub(st))
		}
	}()
	return d.db.Query(query, args...)
}

func (d *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	st := time.Now()
	defer func() {
		if d.debug {
			et := time.Now()
			d.log.Println(query, args, et.Sub(st))
		}
	}()
	return d.db.QueryRow(query, args...)
}
func (d *DB) Close() error {
	return d.db.Close()
}

func (d *DB) Begin() (t *Tx, err error) {
	tx, err := d.db.Begin()
	if err != nil {
		return
	}
	t = &Tx{
		log: d.log,
		tx:  tx,
	}
	return
}
func (d *DB) Prepare(query string) (*Stmt, error) {
	s, err := d.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &Stmt{
		log:     d.log,
		stmt:    s,
		prepare: query,
		debug:   d.debug,
	}, nil
}
