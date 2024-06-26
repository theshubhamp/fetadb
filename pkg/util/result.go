package util

import (
	"encoding/json"
	"fmt"
	pgx "github.com/jackc/pgx/v5/pgproto3"
)

func ToRowDescription(dataframe *DataFrame) *pgx.RowDescription {
	fields := []pgx.FieldDescription{}

	if dataframe == nil {
		return &pgx.RowDescription{
			Fields: []pgx.FieldDescription{},
		}
	}

	columnId := 0
	for _, column := range *dataframe {
		columnName := ""
		if column.Name != "" {
			columnName = column.Name
		}
		if columnName == "" {
			columnName = fmt.Sprintf("res%v", columnId)
			columnId++
		}

		fields = append(fields, pgx.FieldDescription{
			Name: []byte(columnName),
		})
	}

	return &pgx.RowDescription{
		Fields: fields,
	}
}

func ToDataRows(dataframe *DataFrame) []pgx.DataRow {
	if dataframe == nil {
		return []pgx.DataRow{}
	}

	numCols := len(*dataframe)
	numRows := len((*dataframe)[0].Items)

	rows := []pgx.DataRow{}

	for idxRow := range numRows {
		columns := [][]byte{}
		for idxCol := range numCols {
			marshalled, _ := json.Marshal((*dataframe)[idxCol].Items[idxRow])
			columns = append(columns, marshalled)
		}
		rows = append(rows, pgx.DataRow{Values: columns})
	}

	return rows
}
