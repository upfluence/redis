package redisutil

import (
	"net/url"

	"github.com/upfluence/redis"
	"github.com/upfluence/redis/backend"
)

type Option func(*builder)

func WithConfig(fn func(*backend.Config)) Option {
	return func(b *builder) { fn(&b.cfg) }
}

func WithPrefix(prefix string) Option {
	return func(b *builder) { b.prefix = prefix }
}

func WithMiddleware(f redis.MiddlewareFactory) Option {
	return func(b *builder) { b.middlewares = append(b.middlewares, f) }
}

func WithURL(v string) Option {
	var (
		cfg backend.Config

		u, err = url.Parse(v)
	)

	if err == nil {
		cfg, err = backend.ParseURL(v)
	}

	return func(b *builder) {
		if err != nil {
			b.err = err
			return
		}

		b.prefix = u.Query().Get("prefix")
		b.cfg = cfg

	}
}

type builder struct {
	cfg backend.Config

	prefix      string
	middlewares []redis.MiddlewareFactory

	err error
}

func Open(opts ...Option) (redis.DB, error) {
	b := builder{
		cfg: backend.Config{
			Network: "tcp",
			Addr:    "localhost:6789",
			DB:      0,
		},
	}

	for _, opt := range opts {
		opt(&b)
	}

	if b.err != nil {
		return nil, b.err
	}

	db := backend.NewDBFromConfig(b.cfg)

	for _, m := range b.middlewares {
		db = m.Wrap(db)
	}

	return db, nil
}