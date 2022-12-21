package sqlitecx

import (
	"sync"
	"time"

	"crawshaw.io/sqlite"
)

type Rows struct {
	s         *sqlite.Stmt
	err       error
	closeOnce sync.Once
	transient bool
}

func (s *Rows) Close() error {
	s.closeOnce.Do(func() {
		if s.transient {
			s.err = s.s.Finalize()
		} else {
			s.err = s.s.ClearBindings()
		}
	})
	return s.err
}

func (s *Rows) Next() bool {
	hasRow, err := s.s.Step()
	if err != nil {
		s.err = err
	}
	return hasRow
}

func (s *Rows) Err() error {
	return s.err
}

func (s *Rows) Columns() ([]string, error) {
	var result []string
	cnt := s.s.ColumnCount()
	for i := 0; i < cnt; i++ {
		result = append(result, s.s.ColumnName(i))
	}
	return result, nil
}

func (s *Rows) Scan(dest ...interface{}) error {
	for i, v := range dest {
		switch d := v.(type) {
		case *int:
			*d = s.s.ColumnInt(i)
		case *int8:
			*d = int8(s.s.ColumnInt(i))
		case *int16:
			*d = int16(s.s.ColumnInt(i))
		case *int32:
			*d = s.s.ColumnInt32(i)
		case *int64:
			*d = s.s.ColumnInt64(i)
		case *uint:
			*d = uint(s.s.ColumnInt(i))
		case *uint8:
			*d = uint8(s.s.ColumnInt(i))
		case *uint16:
			*d = uint16(s.s.ColumnInt(i))
		case *uint32:
			*d = uint32(s.s.ColumnInt32(i))
		case *uint64:
			*d = uint64(s.s.ColumnInt64(i))
		case *bool:
			*d = s.s.ColumnInt(i) != 0
		case *string:
			*d = s.s.ColumnText(i)
		case *[]byte:
			*d = StmtGetBytes(s.s, i)
		case *float32:
			*d = float32(s.s.ColumnFloat(i))
		case *float64:
			*d = s.s.ColumnFloat(i)
		case *time.Time:
			switch s.s.ColumnType(i) {
			case sqlite.SQLITE_INTEGER:
				*d = time.Unix(s.s.ColumnInt64(i), 0)
			case sqlite.SQLITE_FLOAT:
				*d = FromSQLiteTime(s.s.ColumnFloat(i))
			case sqlite.SQLITE_TEXT:
				*d = stringToTimeAnyText(s.s.ColumnText(i))
			case sqlite.SQLITE_BLOB:
				*d = stringToTimeAnyText(string(StmtGetBytes(s.s, i)))
			case sqlite.SQLITE_NULL:
				*d = time.Time{}
			}
		}
	}
	return nil
}

func stringToTimeAnyText(s string) time.Time {
	for _, v := range textTimeFormats {
		r, err := time.Parse(v, s)
		if err == nil {
			return r
		}
	}
	return time.Time{}
}

var textTimeFormats = []string{
	"2006-01-02 15:04",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04:05.999",
	"2006-01-02T15:04",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05.999",
}

type QueryPrep struct {
	prepFn func(s *sqlite.Stmt)
}

func PrepFunc(prepFn func(s *sqlite.Stmt)) QueryPrep {
	return QueryPrep{prepFn: prepFn}
}

func NoPrep() QueryPrep { return QueryPrep{} }
