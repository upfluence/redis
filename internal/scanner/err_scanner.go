package scanner

type ErrScanner struct {
	Err error
}

func (es *ErrScanner) Scan(_ ...interface{}) error { return es.Err }
