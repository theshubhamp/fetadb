package types

import (
	"encoding/json"
	"fetadb/pkg/util/types/dataframe"
	pgproto "github.com/jackc/pgx/v5/pgproto3"
)

func ToRowDescription(dataframe *dataframe.DataFrame) *pgproto.RowDescription {
	fields := []pgproto.FieldDescription{}

	if dataframe == nil {
		return &pgproto.RowDescription{
			Fields: []pgproto.FieldDescription{},
		}
	}

	for _, column := range dataframe.Columns() {
		fields = append(fields, pgproto.FieldDescription{
			Name: []byte(column.ColumnRef().String()),
		})
	}

	return &pgproto.RowDescription{
		Fields: fields,
	}
}

func ToDataRows(dataframe *dataframe.DataFrame) []pgproto.DataRow {
	if dataframe == nil {
		return []pgproto.DataRow{}
	}

	numCols := dataframe.ColCount()
	numRows := dataframe.RowCount()

	rows := []pgproto.DataRow{}

	for idxRow := range numRows {
		columns := [][]byte{}
		for idxCol := range numCols {
			marshalled, _ := json.Marshal(dataframe.GetColumn(idxCol).Get(idxRow))
			columns = append(columns, marshalled)
		}
		rows = append(rows, pgproto.DataRow{Values: columns})
	}

	return rows
}
