package scanner

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"github.com/upfluence/redis"
)

var errNilPtr = errors.New("destination pointer is nil")

func convertAssign(dest, src any) error {
	if sc, ok := dest.(redis.ValueScanner); ok {
		return sc.Scan(src)
	}

	// Common cases, without reflect.
	switch s := src.(type) {
	case string:
		switch d := dest.(type) {
		case *string:
			if d == nil {
				return errNilPtr
			}
			*d = s
			return nil
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = []byte(s)
			return nil
		}
	case []byte:
		switch d := dest.(type) {
		case *string:
			if d == nil {
				return errNilPtr
			}
			*d = string(s)
			return nil
		case *any:
			if d == nil {
				return errNilPtr
			}
			*d = bytes.Clone(s)
			return nil
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = bytes.Clone(s)
			return nil
		}
	case nil:
		return redis.Empty
	}

	var sv reflect.Value

	switch d := dest.(type) {
	case *string:
		sv = reflect.ValueOf(src)
		switch sv.Kind() {
		case reflect.Bool,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64:
			*d = asString(src)
			return nil
		}
	case *[]byte:
		sv = reflect.ValueOf(src)
		if b, ok := asBytes(sv); ok {
			*d = b
			return nil
		}
	case *bool:
		bv, err := driver.Bool.ConvertValue(src)
		if err == nil {
			*d = bv.(bool)
		}
		return err
	case *any:
		*d = src
		return nil
	}

	dpv := reflect.ValueOf(dest)

	if dpv.Kind() != reflect.Pointer {
		return errors.New("destination not a pointer")
	}

	if dpv.IsNil() {
		return errNilPtr
	}

	if !sv.IsValid() {
		sv = reflect.ValueOf(src)
	}

	dv := reflect.Indirect(dpv)

	if sv.IsValid() && sv.Type().AssignableTo(dv.Type()) {
		switch b := src.(type) {
		case []byte:
			dv.Set(reflect.ValueOf(bytes.Clone(b)))
		default:
			dv.Set(sv)
		}
		return nil
	}

	if dv.Kind() == sv.Kind() && sv.Type().ConvertibleTo(dv.Type()) {
		dv.Set(sv.Convert(dv.Type()))
		return nil
	}

	switch dv.Kind() {
	case reflect.Pointer:
		if src == nil {
			dv.SetZero()
			return nil
		}
		dv.Set(reflect.New(dv.Type().Elem()))
		return convertAssign(dv.Interface(), src)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if src == nil {
			return fmt.Errorf("converting NULL to %s is unsupported", dv.Kind())
		}
		s := asString(src)
		i64, err := strconv.ParseInt(s, 10, dv.Type().Bits())
		if err != nil {
			err = strconvErr(err)
			return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", src, s, dv.Kind(), err)
		}
		dv.SetInt(i64)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if src == nil {
			return fmt.Errorf("converting NULL to %s is unsupported", dv.Kind())
		}
		s := asString(src)
		u64, err := strconv.ParseUint(s, 10, dv.Type().Bits())
		if err != nil {
			err = strconvErr(err)
			return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", src, s, dv.Kind(), err)
		}
		dv.SetUint(u64)
		return nil
	case reflect.Float32, reflect.Float64:
		if src == nil {
			return fmt.Errorf("converting NULL to %s is unsupported", dv.Kind())
		}
		s := asString(src)
		f64, err := strconv.ParseFloat(s, dv.Type().Bits())
		if err != nil {
			err = strconvErr(err)
			return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", src, s, dv.Kind(), err)
		}
		dv.SetFloat(f64)
		return nil
	case reflect.String:
		if src == nil {
			return fmt.Errorf("converting NULL to %s is unsupported", dv.Kind())
		}
		switch v := src.(type) {
		case string:
			dv.SetString(v)
			return nil
		case []byte:
			dv.SetString(string(v))
			return nil
		}
	}

	return fmt.Errorf("unsupported Scan, storing driver.Value type %T into type %T", src, dest)
}

func strconvErr(err error) error {
	if ne, ok := err.(*strconv.NumError); ok {
		return ne.Err
	}
	return err
}

func asString(src any) string {
	switch v := src.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	}

	rv := reflect.ValueOf(src)

	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float64:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 64)
	case reflect.Float32:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 32)
	case reflect.Bool:
		return strconv.FormatBool(rv.Bool())
	}

	return fmt.Sprintf("%v", src)
}

func asBytes(rv reflect.Value) ([]byte, bool) {
	var buf []byte

	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.AppendInt(buf, rv.Int(), 10), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.AppendUint(buf, rv.Uint(), 10), true
	case reflect.Float32:
		return strconv.AppendFloat(buf, rv.Float(), 'g', -1, 32), true
	case reflect.Float64:
		return strconv.AppendFloat(buf, rv.Float(), 'g', -1, 64), true
	case reflect.Bool:
		return strconv.AppendBool(buf, rv.Bool()), true
	case reflect.String:
		s := rv.String()
		return append(buf, s...), true
	}

	return buf, false
}

func Assign(src any, dsts []any) error {
	switch ssrc := src.(type) {
	case []any:
		if len(dsts) == 1 {
			if sc, ok := dsts[0].(redis.ValueScanner); ok {
				return sc.Scan(src)
			}

			rv := reflect.ValueOf(dsts[0])

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

		if len(dsts) != len(ssrc) {
			return fmt.Errorf("unsupported Scan, storing driver.Value type %T into multiple values", src)
		}

		for i, dst := range dsts {
			if err := convertAssign(dst, ssrc[i]); err != nil {
				return err
			}
		}

		return nil
	case map[any]any:
		if len(dsts) > 1 {
			return fmt.Errorf("unsupported Scan, storing driver.Value type %T into multiple values", src)
		}

		if sc, ok := dsts[0].(redis.ValueScanner); ok {
			return sc.Scan(src)
		}

		rv := reflect.ValueOf(dsts[0])

		if rv.Kind() != reflect.Pointer {
			return errNilPtr
		}

		rv = rv.Elem()

		if rv.Kind() != reflect.Map {
			return fmt.Errorf("unsupported Scan, storing driver.Value type %T into %T", src, dsts[0])
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

	if len(dsts) > 1 {
		return fmt.Errorf("unsupported Scan, storing driver.Value type %T into multiple values", src)
	}

	return convertAssign(dsts[0], src)
}

type StaticScanner struct {
	Val any
}

func (ss *StaticScanner) Scan(vs ...interface{}) error {
	if len(vs) == 0 {
		return nil
	}

	return Assign(ss.Val, vs)
}
