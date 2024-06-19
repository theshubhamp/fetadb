package sql

import (
	"fetadb/pkg/sql/expr"
	"fetadb/pkg/util"
	"fmt"
	pg_query "github.com/pganalyze/pg_query_go/v5"
	"reflect"
)

func ToStatements(parseResult *pg_query.ParseResult) ([]Statement, error) {
	statements := []Statement{}
	for _, stmt := range parseResult.GetStmts() {
		result, err := ToStatement(stmt)
		if err != nil {
			return nil, err
		}

		if result != nil {
			statements = append(statements, result)
		}
	}
	return statements, nil
}

func ToStatement(stmt *pg_query.RawStmt) (Statement, error) {
	if stmt.Stmt.GetSelectStmt() != nil {
		return ToSelect(stmt.Stmt.GetSelectStmt())
	} else if stmt.Stmt.GetCreateStmt() != nil {
		return ToCreate(stmt.Stmt.GetCreateStmt())
	} else if stmt.Stmt.GetInsertStmt() != nil {
		return ToInsert(stmt.Stmt.GetInsertStmt())
	}

	return nil, fmt.Errorf("unsupported statement type: %v", reflect.TypeOf(stmt.Stmt.GetNode()))
}

func ToSelect(selectStmt *pg_query.SelectStmt) (Select, error) {
	targets := []Target{}
	for _, targetItem := range selectStmt.GetTargetList() {
		targetExpr, err := ToExpression(targetItem.GetResTarget().GetVal())
		if err != nil {
			return Select{}, err
		}

		target := Target{
			Name:  targetItem.GetResTarget().GetName(),
			Value: targetExpr,
		}
		targets = append(targets, target)
	}

	froms := []From{}
	for _, fromClause := range selectStmt.GetFromClause() {
		rangeVar := fromClause.GetRangeVar()
		from := From{
			Catalog: rangeVar.GetCatalogname(),
			Schema:  rangeVar.GetSchemaname(),
			Rel:     rangeVar.GetRelname(),
			Alias:   rangeVar.GetAlias().GetAliasname(),
		}
		froms = append(froms, from)
	}

	var where expr.Expression = nil
	if selectStmt.GetWhereClause() != nil {
		whereExpr, err := ToExpression(selectStmt.GetWhereClause())
		if err != nil {
			return Select{}, err
		}
		where = whereExpr
	}

	return Select{
		Targets: targets,
		From:    froms,
		Where:   where,
	}, nil
}

func ToExpression(node *pg_query.Node) (expr.Expression, error) {
	if node.GetColumnRef() != nil {
		refs := []string{}
		for _, field := range node.GetColumnRef().GetFields() {
			if field.GetString_() != nil {
				refs = append(refs, field.GetString_().GetSval())
			} else if field.GetAStar() != nil {
				refs = append(refs, "*")
			} else {
				return nil, fmt.Errorf("unsupported column ref: %v", reflect.TypeOf(field.GetNode()))
			}
		}
		return expr.ColumnRef{Names: refs}, nil
	} else if node.GetAConst() != nil {
		aconst := node.GetAConst()
		if aconst.GetSval() != nil {
			return expr.Literal{Value: aconst.GetSval().GetSval()}, nil
		} else if aconst.GetBoolval() != nil {
			return expr.Literal{Value: aconst.GetBoolval().GetBoolval()}, nil
		} else if aconst.GetIval() != nil {
			return expr.Literal{Value: aconst.GetIval().GetIval()}, nil
		} else if aconst.GetFval() != nil {
			return expr.Literal{Value: aconst.GetFval().GetFval()}, nil
		}
	} else if node.GetAExpr() != nil {
		switch node.GetAExpr().GetKind() {
		case pg_query.A_Expr_Kind_AEXPR_OP,
			pg_query.A_Expr_Kind_AEXPR_LIKE,
			pg_query.A_Expr_Kind_AEXPR_ILIKE,
			pg_query.A_Expr_Kind_AEXPR_OP_ALL,
			pg_query.A_Expr_Kind_AEXPR_OP_ANY:
			operator := node.GetAExpr().GetName()[0].GetString_().GetSval()

			leftExpr, err := ToExpression(node.GetAExpr().GetLexpr())
			if err != nil {
				return nil, err
			}
			rightExpr, err := ToExpression(node.GetAExpr().GetRexpr())
			if err != nil {
				return nil, err
			}

			return expr.NewBinaryOperator(operator, leftExpr, rightExpr), nil
		}
	}

	return nil, fmt.Errorf("unspported node: %v", reflect.TypeOf(node.GetNode()))
}

func ToCreate(createStatement *pg_query.CreateStmt) (Create, error) {
	table := TableDef{
		Catalog: createStatement.GetRelation().GetCatalogname(),
		Schema:  createStatement.GetRelation().GetSchemaname(),
		Rel:     createStatement.GetRelation().GetRelname(),
		Alias:   createStatement.GetRelation().GetAlias().GetAliasname(),
	}

	columnDefs := []ColumnDef{}
	primaryColumn := ""
	for _, tableElts := range createStatement.GetTableElts() {
		columnKind, err := util.LookupKind(tableElts.GetColumnDef().GetTypeName().GetNames()[0].GetString_().GetSval())
		if err != nil {
			return Create{}, err
		}

		columnDef := ColumnDef{
			Name:    tableElts.GetColumnDef().GetColname(),
			Type:    columnKind,
			Primary: false,
			NotNull: false,
		}

		if tableElts.GetColumnDef().GetConstraints() != nil {
			for _, constraint := range tableElts.GetColumnDef().GetConstraints() {
				switch constraint.GetConstraint().GetContype() {
				case pg_query.ConstrType_CONSTR_NULL:
					columnDef.NotNull = false
				case pg_query.ConstrType_CONSTR_NOTNULL:
					columnDef.NotNull = true
				case pg_query.ConstrType_CONSTR_PRIMARY:
					if primaryColumn != "" {
						return Create{}, fmt.Errorf("duplicate primary key column %v, previous %v", columnDef.Name, primaryColumn)
					}
					primaryColumn = columnDef.Name
					columnDef.Primary = true
					columnDef.NotNull = true
				}
			}
		}

		columnDefs = append(columnDefs, columnDef)
	}

	return Create{
		Table:   table,
		Columns: columnDefs,
	}, nil
}

func ToInsert(insertStatement *pg_query.InsertStmt) (Insert, error) {
	table := TargetTable{
		Catalog: insertStatement.GetRelation().GetCatalogname(),
		Schema:  insertStatement.GetRelation().GetSchemaname(),
		Rel:     insertStatement.GetRelation().GetRelname(),
		Alias:   insertStatement.GetRelation().GetAlias().GetAliasname(),
	}

	requestedColumns := []RequestedColumn{}
	values := [][]expr.Expression{}

	for _, colNode := range insertStatement.GetCols() {
		requestedColumns = append(requestedColumns, RequestedColumn{
			Name: colNode.GetResTarget().GetName(),
		})
	}

	for _, val := range insertStatement.GetSelectStmt().GetSelectStmt().GetValuesLists() {
		row := []expr.Expression{}
		for _, col := range val.GetList().GetItems() {
			valueExpr, err := ToExpression(col)
			if err != nil {
				return Insert{}, err
			}
			row = append(row, valueExpr)
		}
		values = append(values, row)
	}

	return Insert{
		Table:  table,
		Column: requestedColumns,
		Values: values,
	}, nil
}
