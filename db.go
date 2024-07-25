package redis

import (
	"context"
	"errors"
	"io"
)

var Empty = errors.New("empty")

type Valuer interface {
	Value() (interface{}, error)
}

type ValueScanner interface {
	Scan(interface{}) error
}

type Scanner interface {
	Scan(...interface{}) error
}

type DB interface {
	io.Closer

	Do(context.Context, string, ...interface{}) Scanner
}

type MiddlewareFactory interface {
	Wrap(DB) DB
}
