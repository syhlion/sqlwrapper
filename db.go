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
	slow  time.Duration
}

func (t *Tx) Commit() error {
	st := time.Now()
	defer func() {
		et := time.Now()
		total := et.Sub(st)
		if t.debug || total >= t.slow {
			t.log.Println("commit", total)
		}
	}()
	return t.tx.Commit()
}
func (t *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	st := time.Now()
	defer func() {
		et := time.Now()
		total := et.Sub(st)
		if t.debug || total >= t.slow {
			t.log.Println(query, args, total)
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
		et := time.Now()
		total := et.Sub(st)
		if t.debug || total >= t.slow {
			t.log.Println("rollback", total)
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
		et := time.Now()
		total := et.Sub(st)
		if t.debug || total >= t.slow {
			t.log.Println(query, args, total)
		}
	}()
	return t.tx.Query(query, args...)
}
func (t *Tx) QueryRow(query string, args ...interface{}) *sql.Row {
	st := time.Now()
	defer func() {
		et := time.Now()
		total := et.Sub(st)
		if t.debug || total >= t.slow {
			t.log.Println(query, args, total)
		}
	}()
	return t.tx.QueryRow(query, args...)
}

type Stmt struct {
	log     *log.Logger
	stmt    *sql.Stmt
	prepare string
	debug   bool
	slow    time.Duration
}

func (s *Stmt) Exec(args ...interface{}) (sql.Result, error) {
	st := time.Now()
	defer func() {
		et := time.Now()
		total := et.Sub(st)
		if s.debug || total >= s.slow {
			s.log.Println(s.prepare, args, total)
		}
	}()
	return s.stmt.Exec(args...)
}
func (s *Stmt) Query(args ...interface{}) (*sql.Rows, error) {
	st := time.Now()
	defer func() {
		et := time.Now()
		total := et.Sub(st)
		if s.debug || total >= s.slow {
			s.log.Println(s.prepare, args, total)
		}
	}()
	return s.stmt.Query(args...)
}
func (s *Stmt) QueryRow(args ...interface{}) *sql.Row {
	st := time.Now()
	defer func() {
		et := time.Now()
		total := et.Sub(st)
		if s.debug || total >= s.slow {
			s.log.Println(s.prepare, args, total)
		}
	}()
	return s.stmt.QueryRow(args...)
}
func (s *Stmt) Close() error {
	return s.stmt.Close()
}

type DB struct {
	db    *sql.DB
	log   *log.Logger
	slow  time.Duration
	debug bool
}

func WrapperDB(db *sql.DB, debug bool, slow time.Duration) (d *DB) {
	l := log.New(os.Stdout, "[sql]", log.LstdFlags)

	return &DB{
		db:    db,
		slow:  slow,
		debug: debug,
		log:   l,
	}
}
func (d *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	st := time.Now()
	defer func() {
		et := time.Now()
		total := et.Sub(st)
		if d.debug || total >= d.slow {
			d.log.Println(query, args, total)
		}
	}()
	return d.db.Exec(query, args...)

}

func (d *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	st := time.Now()
	defer func() {
		et := time.Now()
		total := et.Sub(st)
		if d.debug || total >= d.slow {
			d.log.Println(query, args, total)
		}
	}()
	return d.db.Query(query, args...)
}

func (d *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	st := time.Now()
	defer func() {
		et := time.Now()
		total := et.Sub(st)
		if d.debug || total >= d.slow {
			d.log.Println(query, args, total)
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
		log:   d.log,
		tx:    tx,
		debug: d.debug,
		slow:  d.slow,
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
		slow:    d.slow,
	}, nil
}
