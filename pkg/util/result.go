package util

import (
	"encoding/json"
	pgx "github.com/jackc/pgx/v5/pgproto3"
	"strconv"
)

func ToRowDescription(dataframe DataFrame) *pgx.RowDescription {
	fields := []pgx.FieldDescription{}
	for _, column := range dataframe {
		fields = append(fields, pgx.FieldDescription{
			Name: []byte(strconv.FormatUint(column.ID, 10)),
		})
	}

	return &pgx.RowDescription{
		Fields: fields,
	}
}

func ToDataRows(dataframe DataFrame) []pgx.DataRow {
	numCols := len(dataframe)
	numRows := len(dataframe[0].Items)

	rows := []pgx.DataRow{}

	for idxRow := range numRows {
		columns := [][]byte{}
		for idxCol := range numCols {
			marshalled, _ := json.Marshal(dataframe[idxCol].Items[idxRow])
			columns = append(columns, marshalled)
		}
		rows = append(rows, pgx.DataRow{Values: columns})
	}

	return rows
}
