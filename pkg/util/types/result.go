package types

import (
	"encoding/json"
	"fetadb/pkg/util/types/dataframe"
	pgx "github.com/jackc/pgx/v5/pgproto3"
)

func ToRowDescription(dataframe *dataframe.DataFrame) *pgx.RowDescription {
	fields := []pgx.FieldDescription{}

	if dataframe == nil {
		return &pgx.RowDescription{
			Fields: []pgx.FieldDescription{},
		}
	}

	for _, column := range dataframe.Columns() {
		fields = append(fields, pgx.FieldDescription{
			Name: []byte(column.ColumnRef().String()),
		})
	}

	return &pgx.RowDescription{
		Fields: fields,
	}
}

func ToDataRows(dataframe *dataframe.DataFrame) []pgx.DataRow {
	if dataframe == nil {
		return []pgx.DataRow{}
	}

	numCols := dataframe.ColCount()
	numRows := dataframe.RowCount()

	rows := []pgx.DataRow{}

	for idxRow := range numRows {
		columns := [][]byte{}
		for idxCol := range numCols {
			marshalled, _ := json.Marshal(dataframe.GetColumn(idxCol).Get(idxRow))
			columns = append(columns, marshalled)
		}
		rows = append(rows, pgx.DataRow{Values: columns})
	}

	return rows
}
