package backend_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/upfluence/redis"
	"github.com/upfluence/redis/redistest"
)

func TestIntegration(t *testing.T) {
	redistest.NewTestCase().Run(t, func(t *testing.T, db redis.DB) {
		var (
			foo string

			ctx = context.Background()
		)

		err := db.Do(ctx, "SET", "foo", "bar").Scan(&foo)

		require.NoError(t, err)
		assert.Equal(t, "OK", foo)

		err = db.Do(ctx, "GET", "foo").Scan(&foo)

		require.NoError(t, err)
		assert.Equal(t, "bar", foo)

		err = db.Do(ctx, "HSET", "hfoo", "foo", "bar").Scan()

		require.NoError(t, err)

		var hfoo = make(map[string]string)

		err = db.Do(ctx, "HGETALL", "hfoo").Scan(&hfoo)

		require.NoError(t, err)
		assert.Equal(t, map[string]string{"foo": "bar"}, hfoo)

		var sfoo []string

		err = db.Do(ctx, "HKEYS", "hfoo").Scan(&sfoo)
		require.NoError(t, err)
		assert.Equal(t, []string{"foo"}, sfoo)

		err = db.Do(ctx, "HLEN", "hfoo").Scan(&foo)
		require.NoError(t, err)
		assert.Equal(t, "1", foo)

		sfoo = nil
		err = db.Do(ctx, "ZMPOP", 1, "foob", "MIN").Scan(&sfoo)

		require.ErrorIs(t, err, redis.Empty)
		assert.Len(t, sfoo, 0)
	})
}
