package backend

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	goredis "github.com/redis/go-redis/v9"

	"github.com/upfluence/redis"
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
				return errScanner{err: err}
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

	switch ssrc := src.(type) {
	case []any:
		if len(vs) == 1 {
			if sc, ok := vs[0].(redis.ValueScanner); ok {
				return sc.Scan(src)
			}

			rv := reflect.ValueOf(vs[0])

			if rv.Kind() != reflect.Pointer {
				return errNilPtr
			}

			rve := rv.Elem()

			if rve.Kind() == reflect.Slice {
				for _, src := range ssrc {
					rv := reflect.New(rve.Type().Elem())

					if err := convertAssign(rv.Interface(), src); err != nil {
						return err
					}

					rve = reflect.Append(rve, rv.Elem())
				}

				rv.Elem().Set(rve)

				return nil
			}

		}

		if len(vs) != len(ssrc) {
			return fmt.Errorf("unsupported Scan, storing driver.Value type %T into multiple values", src)
		}

		for i, dst := range vs {
			if err := convertAssign(dst, ssrc[i]); err != nil {
				return err
			}
		}

		return nil
	case map[any]any:
		if len(vs) > 1 {
			return fmt.Errorf("unsupported Scan, storing driver.Value type %T into multiple values", src)
		}

		if sc, ok := vs[0].(redis.ValueScanner); ok {
			return sc.Scan(src)
		}

		rv := reflect.ValueOf(vs[0])

		if rv.Kind() != reflect.Pointer {
			return errNilPtr
		}

		rv = rv.Elem()

		if rv.Kind() != reflect.Map {
			return fmt.Errorf("unsupported Scan, storing driver.Value type %T into %T", src, vs[0])
		}

		for k, v := range ssrc {
			rk := reflect.New(rv.Type().Key())
			re := reflect.New(rv.Type().Elem())

			if err := convertAssign(rk.Interface(), k); err != nil {
				return err
			}

			if err := convertAssign(re.Interface(), v); err != nil {
				return err
			}

			rv.SetMapIndex(rk.Elem(), re.Elem())
		}

		return nil
	}

	if len(vs) > 1 {
		return fmt.Errorf("unsupported Scan, storing driver.Value type %T into multiple values", src)
	}

	return convertAssign(vs[0], src)
}

type errScanner struct {
	err error
}

func (es errScanner) Scan(_ ...interface{}) error { return es.err }
