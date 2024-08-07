package logger

import (
	"context"
	"fmt"
	"time"

	"github.com/upfluence/log"
	"github.com/upfluence/log/record"

	"github.com/upfluence/redis"
)

type Logger interface {
	Log(string, []interface{}, error, time.Duration, ...record.Field)
}

type simplifiedLogger struct {
	level  record.Level
	logger log.Logger
}

func (l *simplifiedLogger) Log(cmd string, vs []interface{}, err error, d time.Duration, ofs ...record.Field) {
	for _, v := range vs {
		cmd += " "
		cmd += fmt.Sprint(v)
	}

	logger := l.logger.WithFields(log.Field("duration", d))

	if len(ofs) > 0 {
		logger = logger.WithFields(ofs...)
	}

	if err != nil {
		logger = logger.WithError(err)
	}

	logger.Log(l.level, cmd)
}

func NewFactory(l Logger) redis.MiddlewareFactory {
	return &factory{l: l}
}

func NewLevelFactory(l log.Logger, lvl record.Level) redis.MiddlewareFactory {
	return NewFactory(&simplifiedLogger{logger: l, level: lvl})
}

func NewDebugFactory(l log.Logger) redis.MiddlewareFactory {
	return NewLevelFactory(l, record.Debug)
}

type factory struct {
	l Logger
}

func (f *factory) Wrap(db redis.DB) redis.DB {
	return &DB{db: db, l: f.l}
}

type DB struct {
	db redis.DB
	l  Logger
}

func (db *DB) Unwrap() redis.DB {
	return db.db
}

func (db *DB) Close() error {
	return db.db.Close()
}

func (db *DB) Do(ctx context.Context, cmd string, vs ...interface{}) redis.Scanner {
	t0 := time.Now()
	sc := db.db.Do(ctx, cmd, vs...)

	return scanner{
		Scanner: sc,
		l:       db.l,
		cmd:     cmd,
		vs:      vs,
		t0:      t0,
	}
}

type scanner struct {
	redis.Scanner

	l   Logger
	cmd string
	vs  []interface{}
	t0  time.Time
}

func (sc scanner) Scan(vs ...interface{}) error {
	err := sc.Scanner.Scan(vs...)

	sc.l.Log(
		sc.cmd,
		sc.vs,
		err,
		time.Since(sc.t0),
	)

	return err
}
