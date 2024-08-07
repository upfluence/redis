package prefix

import (
	"context"
	"fmt"
	"maps"
	"slices"
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

	if pnext, ok := next.(*db); ok {
		vs := maps.Clone(pnext.vs)

		for k, v := range f.vs {
			vs[k] = v
		}

		return &db{
			next:   pnext.next,
			prefix: pnext.prefix + f.prefix,
			vs:     vs,
		}
	}

	return &db{next: next, prefix: f.prefix, vs: f.vs}
}

type db struct {
	next   redis.DB
	prefix string
	vs     map[string]Executor
}

func (db *db) Prefix() string   { return db.prefix }
func (db *db) Unwrap() redis.DB { return db.next }
func (db *db) Close() error     { return db.next.Close() }

func (db *db) Do(ctx context.Context, cmd string, vs ...interface{}) redis.Scanner {
	e, ok := db.vs[cmd]

	if !ok {
		return &scanner.ErrScanner{
			Err: fmt.Errorf("prefix wrapping for cmd %q not implemented", cmd),
		}
	}

	return e.Execute(ctx, db.next, db.prefix, cmd, vs)
}

func Prefix(db redis.DB) string {
	var prefixes []string

	for {
		if pdb, ok := db.(interface{ Prefix() string }); ok {
			prefixes = append(prefixes, pdb.Prefix())
		}

		udb, ok := db.(interface{ Unwrap() redis.DB })

		if !ok {
			slices.Reverse(prefixes)

			return strings.Join(prefixes, "")
		}

		db = udb.Unwrap()
	}
}
