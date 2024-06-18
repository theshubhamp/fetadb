package plan

import (
	"fetadb/pkg/sql"
	"fetadb/pkg/util"
	"fmt"
)

type Aggregate struct {
}

func (a Aggregate) Do() (util.DataFrame, error) {
	return util.DataFrame{}, nil
}

type Append struct {
}

func (a Append) Do() (util.DataFrame, error) {
	return util.DataFrame{}, nil
}

type Result struct {
	Targets []sql.Target
	Child   Node
}

func (r Result) Do() (util.DataFrame, error) {
	if r.Child == nil {
		result := util.DataFrame{}

		for _, target := range r.Targets {
			result = append(result, util.Column{
				ID:    0,
				Name:  target.Name,
				Items: []any{target.Value.Evaluate(nil)},
			})
		}

		return result, nil
	}

	return nil, fmt.Errorf("not supported")
}

type Sort struct {
}

func (s Sort) Do() (util.DataFrame, error) {
	return util.DataFrame{}, nil
}
