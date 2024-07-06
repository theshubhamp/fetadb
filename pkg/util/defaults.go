package util

import (
	"fetadb/pkg/util/types/dataframe"
	"fmt"
)

const (
	DefaultIndex = 0
	TableMeta    = "_meta_"
	TableEval    = "_eval_"
)

var MetaColumnGroup = dataframe.NewColumnRef(fmt.Sprintf("%v.group", TableMeta))
