package sqlitecx

import (
	"context"
	"math"
	"time"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
)

type Executor struct {
	Transient bool
}

func (s Executor) PooledExecute(ctx context.Context, pool *sqlitex.Pool, query string, prepFn func(*sqlite.Stmt), resultFn func(*sqlite.Stmt) error) error {
	conn := pool.Get(ctx)
	if conn == nil {
		return ctx.Err()
	}
	defer pool.Put(conn)

	return s.JustExec(conn, query, prepFn, resultFn)
}

func PooledExecute(ctx context.Context, pool *sqlitex.Pool, query string, prepFn func(*sqlite.Stmt), resultFn func(*sqlite.Stmt) error) error {
	return Executor{}.PooledExecute(ctx, pool, query, prepFn, resultFn)
}

func (s Executor) JustExec(conn *sqlite.Conn, query string, prepFn func(*sqlite.Stmt), resultFn func(*sqlite.Stmt) error) error {
	var stmt *sqlite.Stmt
	var err error

	if s.Transient {
		stmt, _, err = conn.PrepareTransient(query)
	} else {
		stmt, err = conn.Prepare(query)
	}
	if err != nil {
		return err
	}
	if prepFn != nil {
		prepFn(stmt)
	}
	err = execLoop(stmt, resultFn)
	var resetErr error
	if s.Transient {
		resetErr = stmt.Finalize()
	} else {
		resetErr = stmt.Reset()
	}
	if err == nil {
		err = resetErr
	}
	return err
}

func JustExec(conn *sqlite.Conn, query string, prepFn func(*sqlite.Stmt), resultFn func(*sqlite.Stmt) error) error {
	return Executor{}.JustExec(conn, query, prepFn, resultFn)
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
