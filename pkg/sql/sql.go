package sql

import pg_query "github.com/pganalyze/pg_query_go/v5"

func FromParseResult(parseResult *pg_query.ParseResult) []Statement {
	statements := []Statement{}
	for _, stmt := range parseResult.GetStmts() {
		result := FromStmt(stmt)
		if result != nil {
			statements = append(statements, result)
		}
	}
	return statements
}

func FromStmt(stmt *pg_query.RawStmt) Statement {
	if stmt.Stmt.GetSelectStmt() != nil {
		return FromSelectStmt(stmt.Stmt.GetSelectStmt())
	}

	return nil
}

func FromSelectStmt(selectStmt *pg_query.SelectStmt) Select {
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

	return Select{
		From:  froms,
		Where: FromExpr(selectStmt.GetWhereClause().GetAExpr()),
	}
}

func FromExpr(aexpr *pg_query.A_Expr) Expression {
	switch aexpr.GetKind() {
	case pg_query.A_Expr_Kind_AEXPR_OP,
		pg_query.A_Expr_Kind_AEXPR_LIKE,
		pg_query.A_Expr_Kind_AEXPR_ILIKE,
		pg_query.A_Expr_Kind_AEXPR_OP_ALL,
		pg_query.A_Expr_Kind_AEXPR_OP_ANY:
		return FromOperator(aexpr)
	}

	return nil
}

func FromOperator(aexpr *pg_query.A_Expr) Expression {
	op := aexpr.GetName()[0].GetString_().GetSval()

	switch op {
	case "=":
		return Equals{
			Left:  FromOperand(aexpr.GetLexpr()),
			Right: FromOperand(aexpr.GetRexpr()),
		}
	}

	return nil
}

func FromOperand(node *pg_query.Node) Expression {
	if node.GetColumnRef() != nil {
		refs := []string{}
		for _, field := range node.GetColumnRef().Fields {
			refs = append(refs, field.GetString_().GetSval())
		}
		return ColumnRef{Names: refs}
	} else if node.GetAConst() != nil {
		aconst := node.GetAConst()
		if aconst.GetSval() != nil {
			return Literal{Value: aconst.GetSval().GetSval()}
		} else if aconst.GetBoolval() != nil {
			return Literal{Value: aconst.GetBoolval().GetBoolval()}
		} else if aconst.GetIval() != nil {
			return Literal{Value: aconst.GetIval().GetIval()}
		} else if aconst.GetFval() != nil {
			return Literal{Value: aconst.GetFval().GetFval()}
		}
	}

	return nil
}
