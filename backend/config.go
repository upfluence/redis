package backend

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type Config struct {
	Network string
	Addr    string
	DB      int

	ClientName string

	Dial               func(context.Context, string, string) (net.Conn, error)
	ProvideCredentials func(context.Context) (string, string, error)

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolTimeout  time.Duration

	ConnMaxIdleTime time.Duration
	ConnMaxLifeTime time.Duration

	MaxIdleConns int
	MaxOpenConns int
}

func (c Config) toOptions() *goredis.Options {
	return &goredis.Options{
		Network:                    c.Network,
		Addr:                       c.Addr,
		DB:                         c.DB,
		ClientName:                 c.ClientName,
		Dialer:                     c.Dial,
		CredentialsProviderContext: c.ProvideCredentials,
		ContextTimeoutEnabled:      true,
		DialTimeout:                c.DialTimeout,
		ReadTimeout:                c.ReadTimeout,
		WriteTimeout:               c.WriteTimeout,
		PoolTimeout:                c.PoolTimeout,
		ConnMaxIdleTime:            c.ConnMaxIdleTime,
		ConnMaxLifetime:            c.ConnMaxLifeTime,
		MaxIdleConns:               c.MaxIdleConns,
		PoolSize:                   c.MaxOpenConns,
	}
}

func (c Config) DSN() (string, error) {
	var (
		u  url.URL
		vs url.Values
	)

	switch c.Network {
	case "", "tcp":
		u.Scheme = "redis"
		u.Host = c.Addr
		u.Path = "/" + strconv.Itoa(c.DB)
	case "unix":
		u.Scheme = "unix"
		u.Path = c.Addr

		vs = url.Values{"db": {strconv.Itoa(c.DB)}}
	default:
		return "", fmt.Errorf("unsupported network: %q", c.Network)
	}

	if fn := c.ProvideCredentials; fn != nil {
		name, pwd, err := fn(context.Background())

		if err != nil {
			return "", err
		}

		u.User = url.UserPassword(name, pwd)
	}

	if len(vs) > 0 {
		u.RawQuery = vs.Encode()
	}

	return u.String(), nil
}

func ParseURL(url string) (Config, error) {
	opts, err := goredis.ParseURL(url)

	if err != nil {
		return Config{}, err
	}

	c := Config{
		Network:         opts.Network,
		Addr:            opts.Addr,
		DB:              opts.DB,
		ClientName:      opts.ClientName,
		DialTimeout:     opts.DialTimeout,
		ReadTimeout:     opts.ReadTimeout,
		WriteTimeout:    opts.WriteTimeout,
		PoolTimeout:     opts.PoolTimeout,
		ConnMaxIdleTime: opts.ConnMaxIdleTime,
		ConnMaxLifeTime: opts.ConnMaxLifetime,
		MaxIdleConns:    opts.MaxIdleConns,
		MaxOpenConns:    opts.PoolSize,
	}

	if opts.Username != "" || opts.Password != "" {
		c.ProvideCredentials = func(ctx context.Context) (string, string, error) {
			return opts.Username, opts.Password, nil
		}
	}

	return c, nil
}
