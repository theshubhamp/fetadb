package sql

import (
	"fetadb/pkg/sql/expr"
	"fetadb/pkg/sql/stmt"
	"fetadb/pkg/util"
	"fetadb/pkg/util/types"
	"fetadb/pkg/util/types/dataframe"
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
	sources := []stmt.Source{}
	for _, fromClause := range selectStmt.GetFromClause() {
		source, err := ToSource(fromClause)
		if err != nil {
			return stmt.Select{}, err
		}

		sources = append(sources, source)
	}

	var source stmt.Source
	if len(sources) == 0 {
		source = nil
	} else if len(sources) == 1 {
		source = sources[0]
	} else {
		source = sources[0]
		for idx := 1; idx < len(sources); idx++ {
			left := source
			right := sources[idx]

			source = stmt.Join{
				Condition: expr.Literal{Value: true},
				Left:      left,
				Right:     right,
			}
		}
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
		if err != nil {
			return stmt.Select{}, err
		}

		order := dataframe.SortAsc
		if sortByClause.SortbyDir == pg_query.SortByDir_SORTBY_ASC {
			order = dataframe.SortAsc
		} else if sortByClause.SortbyDir == pg_query.SortByDir_SORTBY_DESC {
			order = dataframe.SortDesc
		}

		sortBy = append(sortBy, stmt.SortBy{Ref: columnRef.(expr.ColumnRef), Order: order})
	}

	groupBy := []expr.ColumnRef{}
	groupedColumns := map[string]bool{}
	for _, groupClause := range selectStmt.GetGroupClause() {
		groupColumn, err := ToExpression(groupClause)
		if err != nil {
			return stmt.Select{}, err
		}
		if _, ok := groupColumn.(expr.ColumnRef); !ok {
			return stmt.Select{}, fmt.Errorf("expected grpup by to contain a column ref, found %v", reflect.TypeOf(groupClause.GetNode()))
		}

		groupBy = append(groupBy, groupColumn.(expr.ColumnRef))
		groupedColumns[groupColumn.(expr.ColumnRef).String()] = true
	}

	targets := []stmt.Target{}
	for _, targetItem := range selectStmt.GetTargetList() {
		targetExpr, err := ToExpression(targetItem.GetResTarget().GetVal())
		if err != nil {
			return stmt.Select{}, err
		}

		target := stmt.Target{
			Name:             targetItem.GetResTarget().GetName(),
			Value:            targetExpr,
			DefaultColumnRef: dataframe.NewColumnRef(fmt.Sprintf("%s.%s", util.TableEval, targetExpr.String())),
		}

		if len(groupBy) > 0 {
			if columnRef, ok := targetExpr.(expr.ColumnRef); ok {
				if columnRef.TableRef() != util.TableMeta {
					if grouped, ok := groupedColumns[columnRef.String()]; !ok || !grouped {
						return stmt.Select{}, fmt.Errorf("non grouped columns cannot be in target list: %v", columnRef)
					}
				}
			}
		}

		targets = append(targets, target)
	}

	return stmt.Select{
		Targets: targets,
		Source:  source,
		Where:   where,
		SortBy:  sortBy,
		GroupBy: groupBy,
	}, nil
}

func ToSource(node *pg_query.Node) (stmt.Source, error) {
	if node.GetRangeVar() != nil {
		return stmt.From{
			Catalog: node.GetRangeVar().GetCatalogname(),
			Schema:  node.GetRangeVar().GetSchemaname(),
			Rel:     node.GetRangeVar().GetRelname(),
			Alias:   node.GetRangeVar().GetAlias().GetAliasname(),
		}, nil
	} else if node.GetJoinExpr() != nil {
		condition, err := ToExpression(node.GetJoinExpr().GetQuals())
		if err != nil {
			return nil, err
		}

		left, err := ToSource(node.GetJoinExpr().GetLarg())
		if err != nil {
			return nil, err
		}

		right, err := ToSource(node.GetJoinExpr().GetRarg())
		if err != nil {
			return nil, err
		}

		joinType := types.JoinInner
		switch node.GetJoinExpr().GetJointype() {
		case pg_query.JoinType_JOIN_INNER:
			joinType = types.JoinInner
		case pg_query.JoinType_JOIN_LEFT:
			joinType = types.JoinLeft
		case pg_query.JoinType_JOIN_RIGHT:
			joinType = types.JoinRight
		case pg_query.JoinType_JOIN_FULL:
			joinType = types.JoinFull
		default:
			return nil, fmt.Errorf("unsupported join type: %v", node.GetJoinExpr().GetJointype().String())
		}

		return stmt.Join{
			Condition: condition,
			Type:      joinType,
			Left:      left,
			Right:     right,
		}, nil
	} else {
		return nil, fmt.Errorf("souce / from clause %v not supported", reflect.TypeOf(node.GetNode()))
	}
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

		ref := expr.ColumnRef(refs)
		if ref.TableRef() == "" {
			return nil, fmt.Errorf("unqualified column ref: %v", ref.String())
		}
		return expr.ColumnRef(refs), nil
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
		} else if aconst.GetVal() == nil {
			return expr.Literal{Value: nil}, nil
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
