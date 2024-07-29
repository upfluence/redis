package backend

import (
	"context"
	"errors"

	goredis "github.com/redis/go-redis/v9"

	"github.com/upfluence/redis"
	internal "github.com/upfluence/redis/internal/scanner"
)

type db struct {
	cl goredis.UniversalClient
}

func NewDB(client goredis.UniversalClient) redis.DB {
	return &db{cl: client}
}

func NewDBFromConfig(c Config) redis.DB {
	return NewDB(goredis.NewClient(c.toOptions()))
}

func (db *db) Close() error {
	return db.cl.Close()
}

func (d *db) Do(ctx context.Context, cmd string, vs ...interface{}) redis.Scanner {
	var svs = make([]interface{}, len(vs)+1)

	svs[0] = cmd

	for i, v := range vs {
		rv := v

		if vv, ok := v.(redis.Valuer); ok {
			var err error

			rv, err = vv.Value()

			if err != nil {
				return &internal.ErrScanner{Err: err}
			}
		}

		svs[i+1] = rv
	}

	return &scanner{cmd: d.cl.Do(ctx, svs...)}
}

type scanner struct {
	cmd *goredis.Cmd
}

func (s *scanner) Scan(vs ...interface{}) error {
	if len(vs) == 0 {
		return s.cmd.Err()
	}

	src, err := s.cmd.Result()

	switch {
	case err == nil:
	case errors.Is(err, goredis.Nil):
		return redis.Empty
	default:
		return err
	}

	return internal.Assign(src, vs)
}
