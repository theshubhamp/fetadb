package sql

type Select struct {
	Targets []Target
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

type Target struct {
	Name  string
	Value Expression
}
