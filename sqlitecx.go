package sqlitecx

import (
	"context"
	"math"
	"time"

	"github.com/georgysavva/scany/v2/dbscan"
	sqlite "github.com/go-llsqlite/crawshaw"
	"github.com/go-llsqlite/crawshaw/sqlitex"
)

type Executor struct {
	Transient bool
}

func (s Executor) PooledExecute(ctx context.Context, pool *sqlitex.Pool, query string, prep QueryPrep, result QueryResult) error {
	conn := pool.Get(ctx)
	if conn == nil {
		return ctx.Err()
	}
	defer pool.Put(conn)

	return s.JustExec(conn, query, prep, result)
}

func PooledExecute(ctx context.Context, pool *sqlitex.Pool, query string, prep QueryPrep, result QueryResult) error {
	return Executor{}.PooledExecute(ctx, pool, query, prep, result)
}

func processQueryPrep(s *sqlite.Stmt, p QueryPrep) {
	if p.prepFn != nil {
		p.prepFn(s)
	} else if p.prepBind.IsValid() {
		bindFields(s, p.prepBind)
	}
}

func (s Executor) query(conn *sqlite.Conn, query string, prep QueryPrep) (*Rows, error) {
	var stmt *sqlite.Stmt
	var err error
	if s.Transient {
		stmt, _, err = conn.PrepareTransient(query)
	} else {
		stmt, err = conn.Prepare(query)
	}
	if err != nil {
		return nil, err
	}
	processQueryPrep(stmt, prep)
	return &Rows{
		s:         stmt,
		transient: s.Transient,
	}, nil
}

type QueryResult struct {
	fn func(*sqlite.Stmt) error

	dbScanAll interface{}
	dbScanOne interface{}
}

func ResultFunc(f func(s *sqlite.Stmt) error) QueryResult {
	return QueryResult{fn: f}
}

func NoResult() QueryResult { return QueryResult{} }

func ScanAll(dst interface{}) QueryResult {
	return QueryResult{dbScanAll: dst}
}

func ScanOne(dst interface{}) QueryResult {
	return QueryResult{dbScanOne: dst}
}

func (s Executor) JustExec(conn *sqlite.Conn, query string, prep QueryPrep, result QueryResult) error {
	rows, err := s.query(conn, query, prep)
	if err != nil {
		return err
	}

	if result.dbScanAll != nil {
		return dbscan.ScanAll(result.dbScanAll, rows)
	} else if result.dbScanOne != nil {
		return dbscan.ScanOne(result.dbScanOne, rows)
	}
	defer rows.Close()
	return execLoop(rows.s, result.fn)
}

func JustExec(conn *sqlite.Conn, query string, prep QueryPrep, result QueryResult) error {
	return Executor{}.JustExec(conn, query, prep, result)
}

func execLoop(stmt *sqlite.Stmt, resultFn func(stmt *sqlite.Stmt) error) error {
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			return nil
		}
		if resultFn == nil {
			continue
		}
		if err := resultFn(stmt); err != nil {
			return err
		}
	}
}

func ToSQLiteTime(t time.Time) float64 {
	return ((float64(t.UnixNano()) / float64(time.Second)) / 86400.0) + 2440587.5
}

func FromSQLiteTime(f float64) time.Time {
	nt := int64(math.Round((f - 2440587.5) * 86400.0 * 1000))
	return time.Unix(0, nt*1000*1000)
}

func StmtGetBytesName(stmt *sqlite.Stmt, index string) []byte {
	n := stmt.GetLen(index)
	if n == 0 {
		return nil
	}
	result := make([]byte, n)
	stmt.GetBytes(index, result)
	return result
}

func StmtGetBytes(stmt *sqlite.Stmt, index int) []byte {
	n := stmt.ColumnLen(index)
	if n == 0 {
		return nil
	}
	result := make([]byte, n)
	stmt.ColumnBytes(index, result)
	return result
}
