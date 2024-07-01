package sql

import (
	"fetadb/pkg/sql/expr"
	"fetadb/pkg/sql/stmt"
	"fetadb/pkg/util"
	"fmt"
	pg_query "github.com/pganalyze/pg_query_go/v5"
	"reflect"
	"strconv"
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

	return nil, fmt.Errorf("unsupported stmt type: %v", reflect.TypeOf(stmt.Stmt.GetNode()))
}

func ToSelect(selectStmt *pg_query.SelectStmt) (stmt.Select, error) {
	targets := []stmt.Target{}
	for _, targetItem := range selectStmt.GetTargetList() {
		targetExpr, err := ToExpression(targetItem.GetResTarget().GetVal())
		if err != nil {
			return stmt.Select{}, err
		}

		target := stmt.Target{
			Name:  targetItem.GetResTarget().GetName(),
			Value: targetExpr,
		}
		targets = append(targets, target)
	}

	froms := []stmt.From{}
	for _, fromClause := range selectStmt.GetFromClause() {
		rangeVar := fromClause.GetRangeVar()
		from := stmt.From{
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
			return stmt.Select{}, err
		}
		where = whereExpr
	}

	sortBy := []stmt.SortBy{}
	for _, sortClause := range selectStmt.GetSortClause() {
		sortByClause := sortClause.GetSortBy()
		if sortByClause.GetNode().GetColumnRef() == nil {
			return stmt.Select{}, fmt.Errorf("expected sort by to contain a column ref")
		}

		columnRef, err := ToExpression(sortByClause.GetNode())
		if sortByClause.GetNode().GetColumnRef() == nil {
			return stmt.Select{}, err
		}

		order := util.SortAsc
		if sortByClause.SortbyDir == pg_query.SortByDir_SORTBY_ASC {
			order = util.SortAsc
		} else if sortByClause.SortbyDir == pg_query.SortByDir_SORTBY_DESC {
			order = util.SortDesc
		}

		sortBy = append(sortBy, stmt.SortBy{Ref: columnRef.(expr.ColumnRef), Order: order})
	}

	return stmt.Select{
		Targets: targets,
		From:    froms,
		Where:   where,
		SortBy:  sortBy,
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
			parsedFloat, err := strconv.ParseFloat(aconst.GetFval().GetFval(), 64)
			return expr.Literal{Value: parsedFloat}, err
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

			return expr.NewBinaryOperator(operator, leftExpr, rightExpr)
		}
	} else if node.GetFuncCall() != nil {
		name := node.GetFuncCall().GetFuncname()[0].GetString_().GetSval()
		args := []expr.Expression{}

		for _, argNode := range node.GetFuncCall().GetArgs() {
			arg, err := ToExpression(argNode)
			if err != nil {
				return nil, err
			}
			args = append(args, arg)
		}

		return expr.NewFuncCall(name, args)
	}

	return nil, fmt.Errorf("unspported node: %v", reflect.TypeOf(node.GetNode()))
}

func ToCreate(createStatement *pg_query.CreateStmt) (stmt.Create, error) {
	table := stmt.TableDef{
		Catalog: createStatement.GetRelation().GetCatalogname(),
		Schema:  createStatement.GetRelation().GetSchemaname(),
		Rel:     createStatement.GetRelation().GetRelname(),
		Alias:   createStatement.GetRelation().GetAlias().GetAliasname(),
	}

	columnDefs := []stmt.ColumnDef{}
	primaryColumn := ""
	for _, tableElts := range createStatement.GetTableElts() {
		columnKind, err := util.LookupKind(tableElts.GetColumnDef().GetTypeName().GetNames()[0].GetString_().GetSval())
		if err != nil {
			return stmt.Create{}, err
		}

		columnDef := stmt.ColumnDef{
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
						return stmt.Create{}, fmt.Errorf("duplicate primary key column %v, previous %v", columnDef.Name, primaryColumn)
					}
					primaryColumn = columnDef.Name
					columnDef.Primary = true
					columnDef.NotNull = true
				}
			}
		}

		columnDefs = append(columnDefs, columnDef)
	}

	return stmt.Create{
		Table:   table,
		Columns: columnDefs,
	}, nil
}

func ToInsert(insertStatement *pg_query.InsertStmt) (stmt.Insert, error) {
	table := stmt.TargetTable{
		Catalog: insertStatement.GetRelation().GetCatalogname(),
		Schema:  insertStatement.GetRelation().GetSchemaname(),
		Rel:     insertStatement.GetRelation().GetRelname(),
		Alias:   insertStatement.GetRelation().GetAlias().GetAliasname(),
	}

	requestedColumns := []stmt.RequestedColumn{}
	values := [][]expr.Expression{}

	for _, colNode := range insertStatement.GetCols() {
		requestedColumns = append(requestedColumns, stmt.RequestedColumn{
			Name: colNode.GetResTarget().GetName(),
		})
	}

	for _, val := range insertStatement.GetSelectStmt().GetSelectStmt().GetValuesLists() {
		row := []expr.Expression{}
		for _, col := range val.GetList().GetItems() {
			valueExpr, err := ToExpression(col)
			if err != nil {
				return stmt.Insert{}, err
			}
			row = append(row, valueExpr)
		}
		values = append(values, row)
	}

	return stmt.Insert{
		Table:  table,
		Column: requestedColumns,
		Values: values,
	}, nil
}
