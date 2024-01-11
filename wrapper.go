package sqlitecx

import (
	"context"
	"time"

	sqlite "github.com/go-llsqlite/crawshaw"
	"github.com/go-llsqlite/crawshaw/sqlitex"
)

type Wrapper struct {
	db *sqlitex.Pool

	closed chan struct{}

	housekeepingContext context.Context
	stop                func()
	closeErr            error
}

type Builder struct {
	name     string
	flags    sqlite.OpenFlags
	poolSize int

	migrateDatabase func(*sqlite.Conn) error
}

func NewBuilder(name string) *Builder {
	return &Builder{
		name:     name,
		flags:    sqlite.OpenFlagsDefault,
		poolSize: 3,
	}
}

func (b *Builder) WithFlags(flags sqlite.OpenFlags) *Builder {
	b.flags = flags
	return b
}

func (b *Builder) WithPoolSize(poolSize int) *Builder {
	b.poolSize = poolSize
	return b
}

func (b *Builder) WithMigrateDatabase(migrateDatabase func(*sqlite.Conn) error) *Builder {
	b.migrateDatabase = migrateDatabase
	return b
}

func (b *Builder) Build(ctx context.Context) (*Wrapper, error) {
	db, err := sqlitex.Open(b.name, b.flags, b.poolSize)
	if err != nil {
		return nil, err
	}

	if b.migrateDatabase != nil {
		conn := db.Get(ctx)
		if conn == nil {
			db.Close()
			return nil, context.Canceled
		}

		if err := b.migrateDatabase(conn); err != nil {
			db.Put(conn)
			db.Close()
			return nil, err
		}
		db.Put(conn)
	}
	hc, stop := context.WithCancel(context.Background())
	r := &Wrapper{
		db:                  db,
		closed:              make(chan struct{}, 1),
		housekeepingContext: hc,
		stop:                stop,
	}

	go r.run()

	return r, nil
}

func (s *Wrapper) Close() error {
	s.stop()
	<-s.closed
	return s.closeErr
}

func (s *Wrapper) housekeeping() {
	ctx, cancel := context.WithTimeout(s.housekeepingContext, time.Minute)
	defer cancel()
	conn := s.db.Get(ctx)
	if conn == nil {
		return
	}
	defer s.db.Put(conn)

	sqlitex.ExecuteTransient(conn, "PRAGMA optimize", nil)
	sqlitex.ExecuteTransient(conn, "VACUUM", nil)
}

func (s *Wrapper) run() {
	defer func() {
		defer close(s.closed)
		s.closeErr = s.db.Close()
	}()
	for {
		select {
		case <-s.housekeepingContext.Done():
			s.housekeeping()
			return
		case <-time.After(time.Hour):
			s.housekeeping()
		}
	}
}

func (s *Wrapper) DB() *sqlitex.Pool {
	return s.db
}

func (s *Wrapper) RunInTransaction(ctx context.Context, fn func(*sqlite.Conn) error) (err error) {
	conn := s.db.Get(ctx)
	if conn == nil {
		return ctx.Err()
	}
	defer s.db.Put(conn)
	defer sqlitex.Save(conn)(&err)
	err = fn(conn)
	return err
}
