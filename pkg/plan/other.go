package plan

import "fetadb/pkg/internal"

type Aggregate struct {
}

func (a Aggregate) Do() (internal.DataFrame, error) {
	return internal.DataFrame{}, nil
}

type Append struct {
}

func (a Append) Do() (internal.DataFrame, error) {
	return internal.DataFrame{}, nil
}

type Result struct {
}

func (a Result) Do() (internal.DataFrame, error) {
	return internal.DataFrame{}, nil
}

type Sort struct {
}

func (s Sort) Do() (internal.DataFrame, error) {
	return internal.DataFrame{}, nil
}
