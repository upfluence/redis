package prefix

import (
	"context"
	"fmt"
	"maps"
	"strings"

	"github.com/upfluence/pkg/slices"

	"github.com/upfluence/redis"
	"github.com/upfluence/redis/internal/scanner"
)

var (
	stringExecutors = map[string]Executor{
		"APPEND":      staticIndexRewriter(0),
		"DECR":        staticIndexRewriter(0),
		"DECRBY":      staticIndexRewriter(0),
		"GET":         staticIndexRewriter(0),
		"GETDEL":      staticIndexRewriter(0),
		"GETEX":       staticIndexRewriter(0),
		"GETSET":      staticIndexRewriter(0),
		"INCR":        staticIndexRewriter(0),
		"INCRBY":      staticIndexRewriter(0),
		"INCRBYFLOAT": staticIndexRewriter(0),
		"LCS":         staticIndexRewriter(0),
		"MGET":        indexedRewriteExecutors(func(idx int, _ interface{}) bool { return true }),
		"MSET":        indexedRewriteExecutors(func(idx int, _ interface{}) bool { return idx%2 == 0 }),
		"MSETNX":      indexedRewriteExecutors(func(idx int, _ interface{}) bool { return idx%2 == 0 }),
		"PSETEX":      staticIndexRewriter(0),
		"SET":         staticIndexRewriter(0),
		"SETEX":       staticIndexRewriter(0),
		"SETNX":       staticIndexRewriter(0),
		"SETRANGE":    staticIndexRewriter(0),
		"STRLEN":      staticIndexRewriter(0),
		"SUBSTR":      staticIndexRewriter(0),
		"DEL":         indexedRewriteExecutors(func(idx int, _ interface{}) bool { return true }),
	}

	genericExecutors   = map[string]Executor{}
	listExecutors      = map[string]Executor{}
	hashExecutors      = map[string]Executor{}
	sortedSetExecutors = map[string]Executor{
		"ZADD":             staticIndexRewriter(0),
		"ZCOUNT":           staticIndexRewriter(0),
		"ZRANGE":           staticIndexRewriter(0),
		"ZREM":             staticIndexRewriter(0),
		"ZCARD":            staticIndexRewriter(0),
		"ZSCORE":           staticIndexRewriter(0),
		"ZSCAN":            staticIndexRewriter(0),
		"ZRANK":            staticIndexRewriter(0),
		"ZREVRANK":         staticIndexRewriter(0),
		"ZREMRANGEBYSCORE": staticIndexRewriter(0),
		"ZREMRANGEBYRANK":  staticIndexRewriter(0),
		"ZREMRANGEBYLEX":   staticIndexRewriter(0),
		"ZRANDMEMBER":      staticIndexRewriter(0),
		"ZPOPMIN":          staticIndexRewriter(0),
		"ZPOPMAX":          staticIndexRewriter(0),
		"ZINCRBY":          staticIndexRewriter(0),
		"ZLEXCOUNT":        staticIndexRewriter(0),
		"ZREVRANGE":        staticIndexRewriter(0),
		"ZDIFF":            indexedRewriteExecutors(func(idx int, _ interface{}) bool { return idx > 0 }),
		"ZDIFFSTORE":       indexedRewriteExecutors(func(idx int, _ interface{}) bool { return idx == 0 || idx > 1 }),
		"ZRANGESTORE":      indexedRewriteExecutors(func(idx int, _ interface{}) bool { return idx < 2 }),
		"ZMSCORE":          staticIndexRewriter(0),
	}

	serverExecutors = map[string]Executor{
		"FLUSHDB": flushDBExecutor{},
		"KEYS":    keysExecutor{},
	}

	allExecutors = func() map[string]Executor {
		res := make(map[string]Executor)

		for _, excs := range []map[string]Executor{
			stringExecutors,
			genericExecutors,
			serverExecutors,
			listExecutors,
			hashExecutors,
			sortedSetExecutors,
		} {
			maps.Copy(res, excs)
		}

		return res
	}()
)

type Executor interface {
	Execute(context.Context, redis.DB, string, string, []interface{}) redis.Scanner
}

type keysExecutor struct{}

func (keysExecutor) Execute(ctx context.Context, db redis.DB, prefix string, cmd string, vs []interface{}) redis.Scanner {
	if len(vs) != 1 {
		return &scanner.ErrScanner{Err: fmt.Errorf("invalid number of args: %+v", vs)}
	}

	return &keysScanner{
		sc:     db.Do(ctx, cmd, prefix+stringify(vs[0])),
		prefix: prefix,
	}
}

type keysScanner struct {
	sc     redis.Scanner
	prefix string
}

func (ks *keysScanner) Scan(vs ...interface{}) error {
	var keys []string

	if err := ks.sc.Scan(&keys); err != nil {
		return err
	}

	for i, k := range keys {
		keys[i] = strings.TrimPrefix(k, ks.prefix)
	}

	return scanner.Assign(keys, vs)
}

type flushDBExecutor struct{}

func (flushDBExecutor) Execute(ctx context.Context, db redis.DB, prefix string, cmd string, vs []interface{}) redis.Scanner {
	var keys []string

	if err := db.Do(ctx, "KEYS", prefix+"*").Scan(&keys); err != nil {
		return &scanner.ErrScanner{Err: err}
	}

	for _, batch := range slices.Batch(keys, 1024) {
		args := slices.Map(batch, func(v string) any { return v })

		if err := db.Do(ctx, "DEL", args...).Scan(); err != nil {
			return &scanner.ErrScanner{Err: err}
		}
	}

	return &scanner.StaticScanner{Val: "OK"}
}

func staticIndexRewriter(idxs ...int) Executor {
	idxSet := slices.Unique(idxs)

	return indexedRewriteExecutors(func(idx int, _ interface{}) bool {
		_, ok := idxSet[idx]
		return ok
	})
}

type indexedRewriteExecutors func(int, interface{}) bool

func (fn indexedRewriteExecutors) Execute(ctx context.Context, db redis.DB, prefix string, cmd string, vs []interface{}) redis.Scanner {
	vvs := make([]interface{}, len(vs))

	for i, v := range vs {
		if fn(i, v) {
			vvs[i] = prefix + stringify(v)
		} else {
			vvs[i] = v
		}
	}

	return db.Do(ctx, cmd, vvs...)
}

func stringify(v any) string {
	return fmt.Sprint(v)
}
