package sqlwrapper

import (
	"database/sql"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

func init() {
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.DebugLevel)
}

type Tx struct {
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
			log.WithFields(log.Fields{
				"use-time": total,
				"name":     "syhlion/sqlwrapper",
			}).Debug("tx commit")
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
			log.WithFields(log.Fields{
				"use-time": total,
				"sql":      query,
				"args":     args,
				"name":     "syhlion/sqlwrapper",
			}).Debug("tx exec")
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
		stmt:    s,
		debug:   t.debug,
		prepare: query,
		slow:    t.slow,
	}
	return stmt, nil
}
func (t *Tx) Rollback() error {
	st := time.Now()
	defer func() {
		et := time.Now()
		total := et.Sub(st)
		if t.debug || total >= t.slow {
			log.WithFields(log.Fields{
				"use-time": total,
				"name":     "syhlion/sqlwrapper",
			}).Debug("tx rollback")
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
			log.WithFields(log.Fields{
				"use-time": total,
				"sql":      query,
				"args":     args,
				"name":     "syhlion/sqlwrapper",
			}).Debug("tx query")
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
			log.WithFields(log.Fields{
				"use-time": total,
				"sql":      query,
				"args":     args,
				"name":     "syhlion/sqlwrapper",
			}).Debug("tx query row")
		}
	}()
	return t.tx.QueryRow(query, args...)
}

type Stmt struct {
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
			log.WithFields(log.Fields{
				"use-time": total,
				"args":     args,
				"sql":      s.prepare,
				"name":     "syhlion/sqlwrapper",
			}).Debug("stmt query row")
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
			log.WithFields(log.Fields{
				"use-time": total,
				"args":     args,
				"sql":      s.prepare,
				"name":     "syhlion/sqlwrapper",
			}).Debug("stmt query")
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
			log.WithFields(log.Fields{
				"use-time": total,
				"args":     args,
				"sql":      s.prepare,
				"name":     "syhlion/sqlwrapper",
			}).Debug("stmt query row")
		}
	}()
	return s.stmt.QueryRow(args...)
}
func (s *Stmt) Close() error {
	return s.stmt.Close()
}

type DB struct {
	db    *sql.DB
	slow  time.Duration
	debug bool
}

func WrapperDB(db *sql.DB, debug bool, slow time.Duration) (d *DB) {

	return &DB{
		db:    db,
		slow:  slow,
		debug: debug,
	}
}
func (d *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	st := time.Now()
	defer func() {
		et := time.Now()
		total := et.Sub(st)
		if d.debug || total >= d.slow {
			log.WithFields(log.Fields{
				"use-time": total,
				"args":     args,
				"sql":      query,
				"name":     "syhlion/sqlwrapper",
			}).Debug("db exec")
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
			log.WithFields(log.Fields{
				"use-time": total,
				"args":     args,
				"sql":      query,
				"name":     "syhlion/sqlwrapper",
			}).Debug("db query")
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
			log.WithFields(log.Fields{
				"use-time": total,
				"args":     args,
				"sql":      query,
				"name":     "syhlion/sqlwrapper",
			}).Debug("db query row")
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
		stmt:    s,
		prepare: query,
		debug:   d.debug,
		slow:    d.slow,
	}, nil
}
