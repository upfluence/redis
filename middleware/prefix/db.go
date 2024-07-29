package prefix

import (
	"context"
	"fmt"
	"strings"

	"github.com/upfluence/redis"
	"github.com/upfluence/redis/internal/scanner"
)

type Option func(*factory)

func WithExecutor(cmd string, exc Executor) Option {
	return func(mf *factory) { mf.vs[cmd] = exc }
}

type factory struct {
	prefix string

	vs map[string]Executor
}

func NewFactory(prefix string, opts ...Option) redis.MiddlewareFactory {
	if prefix != "" && !strings.HasSuffix(prefix, ":") {
		prefix = prefix + ":"
	}

	f := factory{prefix: prefix, vs: allExecutors}

	for _, opt := range opts {
		opt(&f)
	}

	return &f
}

func (f *factory) Wrap(next redis.DB) redis.DB {
	if f.prefix == "" {
		return next
	}

	return &db{next: next, prefix: f.prefix, vs: f.vs}
}

type db struct {
	next   redis.DB
	prefix string
	vs     map[string]Executor
}

func (db *db) Close() error { return db.next.Close() }

func (db *db) Do(ctx context.Context, cmd string, vs ...interface{}) redis.Scanner {
	e, ok := db.vs[cmd]

	if !ok {
		return &scanner.ErrScanner{
			Err: fmt.Errorf("prefix wrapping for cmd %q not implemented", cmd),
		}
	}

	return e.Execute(ctx, db.next, db.prefix, cmd, vs)
}
