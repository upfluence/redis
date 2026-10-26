package prefix_test

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/upfluence/redis"
	"github.com/upfluence/redis/middleware/prefix"
	"github.com/upfluence/redis/redistest"
)

func TestIntegration(t *testing.T) {
	redistest.NewTestCase().Run(t, func(t *testing.T, db redis.DB) {
		ctx := context.Background()
		prefixedDB := prefix.NewFactory("foobar").Wrap(db)

		err := prefixedDB.Do(ctx, "SET", "buz", "biz", "PX", 60_000).Scan()
		require.NoError(t, err)

		var ttl int64

		err = prefixedDB.Do(ctx, "TTL", "buz").Scan(&ttl)
		require.NoError(t, err)
		assert.Positive(t, ttl)
		assert.LessOrEqual(t, ttl, int64(60))

		var pttl int64

		err = prefixedDB.Do(ctx, "PTTL", "buz").Scan(&pttl)
		require.NoError(t, err)
		assert.Positive(t, pttl)
		assert.LessOrEqual(t, pttl, int64(60_000))

		var buz string
		err = prefixedDB.Do(ctx, "GET", "buz").Scan(&buz)
		require.NoError(t, err)
		assert.Equal(t, "biz", buz)

		var rawBuz string

		err = db.Do(ctx, "GET", "buz").Scan(&rawBuz)
		assert.ErrorIs(t, redis.Empty, err)
		assert.Equal(t, "", rawBuz)

		err = db.Do(ctx, "GET", "foobar:buz").Scan(&rawBuz)
		require.NoError(t, err)
		assert.Equal(t, "biz", buz)

		err = db.Do(ctx, "SET", "buz", "baz").Scan()
		require.NoError(t, err)

		var keys []string
		err = prefixedDB.Do(ctx, "KEYS", "*").Scan(&keys)
		require.NoError(t, err)
		assert.Equal(t, []string{"buz"}, keys)

		keys = nil
		err = db.Do(ctx, "KEYS", "*").Scan(&keys)
		require.NoError(t, err)

		sort.Strings(keys)
		assert.Equal(t, []string{"buz", "foobar:buz"}, keys)

		err = prefixedDB.Do(ctx, "FLUSHDB").Scan()
		require.NoError(t, err)

		keys = nil
		err = db.Do(ctx, "KEYS", "*").Scan(&keys)
		require.NoError(t, err)
		assert.Equal(t, []string{"buz"}, keys)

		for _, tt := range []struct {
			db   redis.DB
			want string
		}{
			{db: db, want: ""},
			{db: prefixedDB, want: "foobar:"},
			{
				db:   prefix.NewFactory("buz").Wrap(prefixedDB),
				want: "foobar:buz:",
			},
		} {
			assert.Equal(t, tt.want, prefix.Prefix(tt.db))
		}
	})

}
