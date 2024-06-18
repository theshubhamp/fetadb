package sql

import (
	"fetadb/pkg/sql/expr"
	"fetadb/pkg/util"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func ToStatements(parseResult *pg_query.ParseResult) []Statement {
	statements := []Statement{}
	for _, stmt := range parseResult.GetStmts() {
		result := ToStatement(stmt)
		if result != nil {
			statements = append(statements, result)
		}
	}
	return statements
}

func ToStatement(stmt *pg_query.RawStmt) Statement {
	if stmt.Stmt.GetSelectStmt() != nil {
		return ToSelect(stmt.Stmt.GetSelectStmt())
	} else if stmt.Stmt.GetCreateStmt() != nil {
		return ToCreate(stmt.Stmt.GetCreateStmt())
	}

	return nil
}

func ToSelect(selectStmt *pg_query.SelectStmt) Select {
	targets := []Target{}
	for _, targetItem := range selectStmt.GetTargetList() {
		target := Target{
			Name:  targetItem.GetResTarget().GetName(),
			Value: ToExpression(targetItem.GetResTarget().GetVal()),
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

	return Select{
		Targets: targets,
		From:    froms,
		Where:   ToExpression(selectStmt.GetWhereClause()),
	}
}

func ToExpression(node *pg_query.Node) expr.Expression {
	if node.GetColumnRef() != nil {
		refs := []string{}
		for _, field := range node.GetColumnRef().Fields {
			refs = append(refs, field.GetString_().GetSval())
		}
		return expr.ColumnRef{Names: refs}
	} else if node.GetAConst() != nil {
		aconst := node.GetAConst()
		if aconst.GetSval() != nil {
			return expr.Literal{Value: aconst.GetSval().GetSval()}
		} else if aconst.GetBoolval() != nil {
			return expr.Literal{Value: aconst.GetBoolval().GetBoolval()}
		} else if aconst.GetIval() != nil {
			return expr.Literal{Value: aconst.GetIval().GetIval()}
		} else if aconst.GetFval() != nil {
			return expr.Literal{Value: aconst.GetFval().GetFval()}
		}
	} else if node.GetAExpr() != nil {
		switch node.GetAExpr().GetKind() {
		case pg_query.A_Expr_Kind_AEXPR_OP,
			pg_query.A_Expr_Kind_AEXPR_LIKE,
			pg_query.A_Expr_Kind_AEXPR_ILIKE,
			pg_query.A_Expr_Kind_AEXPR_OP_ALL,
			pg_query.A_Expr_Kind_AEXPR_OP_ANY:
			return expr.NewBinaryOperator(
				node.GetAExpr().GetName()[0].GetString_().GetSval(),
				ToExpression(node.GetAExpr().GetLexpr()),
				ToExpression(node.GetAExpr().GetRexpr()),
			)
		}
	}

	return nil
}

func ToCreate(createStatement *pg_query.CreateStmt) Create {
	table := Table{
		Catalog: createStatement.GetRelation().GetCatalogname(),
		Schema:  createStatement.GetRelation().GetSchemaname(),
		Rel:     createStatement.GetRelation().GetRelname(),
		Alias:   createStatement.GetRelation().GetAlias().GetAliasname(),
	}

	columnDefs := []ColumnDef{}
	for _, tableElts := range createStatement.GetTableElts() {
		columnDef := tableElts.GetColumnDef()

		columnDefs = append(columnDefs, ColumnDef{
			Name:    columnDef.GetColname(),
			Type:    util.LookupKind(columnDef.GetTypeName().GetNames()[0].GetString_().GetSval()),
			NotNull: columnDef.GetIsNotNull(),
		})
	}

	return Create{
		Table:   table,
		Columns: columnDefs,
	}
}
