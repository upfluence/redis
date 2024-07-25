package redistest

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/upfluence/log/record"

	"github.com/upfluence/redis"
	"github.com/upfluence/redis/middleware/logger"
	"github.com/upfluence/redis/redisutil"
)

type testLogger struct {
	testing.TB
}

func (tl testLogger) Log(cmd string, vs []interface{}, _ error, d time.Duration, fs ...record.Field) {
	var b strings.Builder

	fmt.Fprintf(&b, "[duration: %v]", d)

	for _, f := range fs {
		fmt.Fprintf(&b, "[%s: %s]", f.GetKey(), f.GetValue())
	}

	b.WriteString(" " + cmd)

	for _, v := range vs {
		fmt.Fprintf(&b, " %v", v)
	}

	tl.TB.Log(b.String())
}

type TestCase struct {
	redisURL string

	opts []redisutil.Option
}

type TestCaseOption func(*TestCase)

func NewTestCase(opts ...TestCaseOption) *TestCase {
	var tc = TestCase{
		redisURL: os.Getenv("REDIS_URL"),
	}

	for _, opt := range opts {
		opt(&tc)
	}

	return &tc
}

func (tc *TestCase) buildDB(t *testing.T) redis.DB {
	db, err := redisutil.Open(
		append(
			tc.opts,
			redisutil.WithURL(tc.redisURL),
			redisutil.WithMiddleware(logger.NewFactory(testLogger{t})),
		)...,
	)

	if err != nil {
		t.Fatalf("Cannot build redis.DB: %+v", err)
	}

	return db
}

func (tc *TestCase) Run(t *testing.T, fn func(t *testing.T, db redis.DB)) {
	t.Helper()

	if tc.redisURL == "" {
		t.Skip("No redis url given, skipping test case")
	}

	db := tc.buildDB(t)

	defer db.Close()

	db.Do(context.Background(), "FLUSHDB")

	fn(t, db)
}
