package sql

type Select struct {
	Targets []any
	From    []From
	Where   Expression
	Having  Expression
}

type From struct {
	Catalog string
	Schema  string
	Rel     string
	Alias   string
}
