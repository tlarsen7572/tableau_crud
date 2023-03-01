package persistance

import (
	"testing"
)

func TestWhereEqual(t *testing.T) {
	clause := EqualClause{Identifier: `field`, Value: `Value`}
	where := clause.ToSqlSnippet()
	expected := `"field"=?`
	if where.Snippet != expected {
		t.Fatalf(`expected "%v" but got "%v"`, expected, where)
	}
	if len(where.Params) != 1 {
		t.Fatalf(`expected 1 param but got %v`, len(where.Params))
	}
	t.Log(where.Snippet)
}

func TestWhereIn(t *testing.T) {
	clause := InClause{
		Identifier: `field`,
		Values: []interface{}{
			`value1`,
			`value2`,
		},
	}
	where := clause.ToSqlSnippet()
	expected := `"field" IN (?,?)`
	if where.Snippet != expected {
		t.Fatalf(`expected where clause of '%v' but got '%v'`, expected, where.Snippet)
	}
	params := where.Params
	if count := len(params); count != 2 {
		t.Fatalf(`expected 2 params but got %v`, count)
	}
	if params[0] != `value1` {
		t.Fatalf(`expected value 'value1' but got '%v'`, params[0])
	}
	if params[1] != `value2` {
		t.Fatalf(`expected value 'value2' but got '%v'`, params[1])
	}
	t.Log(where.Snippet)
}

func TestWhereNotIn(t *testing.T) {
	clause := InClause{
		Identifier: `field`,
		Exclude:    true,
		Values: []interface{}{
			`value1`,
			`value2`,
		},
	}
	where := clause.ToSqlSnippet()
	expected := `NOT ("field" IN (?,?))`
	if where.Snippet != expected {
		t.Fatalf(`expected where clause of '%v' but got '%v'`, expected, where.Snippet)
	}
	t.Log(where.Snippet)
}

func TestWhereRange(t *testing.T) {
	clause := RangeClause{
		Identifier: "field",
		MinValue:   0,
		MaxValue:   10,
	}
	where := clause.ToSqlSnippet()
	expected := `"field" BETWEEN ? AND ?`
	if where.Snippet != expected {
		t.Fatalf(`expected where clause of '%v' but got '%v'`, expected, where.Snippet)
	}
	if count := len(where.Params); count != 2 {
		t.Fatalf(`expected 2 params but got %v`, count)
	}
	param := where.Params[0]
	if param != 0 {
		t.Fatalf(`expected value 0 but got '%v'`, param)
	}
	param = where.Params[1]
	if param != 10 {
		t.Fatalf(`expected value 10 but got '%v'`, param)
	}
	t.Log(where.Snippet)
}

func TestCombineWhereClauses(t *testing.T) {
	clauses := []SqlSnippetGenerator{
		&EqualClause{
			Identifier: "field1",
			Value:      10,
		},
		&InClause{
			Identifier: "field2",
			Exclude:    false,
			Values:     []interface{}{`A`, `B`, `C`},
		},
		&RangeClause{
			Identifier:   `field3`,
			MinValue:     25,
			MaxValue:     30,
			IncludeNulls: false,
		},
	}
	where := GenerateCombinedWhereClause(clauses)
	expected := `"field1"=? AND "field2" IN (?,?,?) AND "field3" BETWEEN ? AND ?`
	if where.Value != expected {
		t.Fatalf(`expected '%v' but got '%v'`, expected, where)
	}
	t.Logf(where.Value)
}

func TestWhereRangeIncludeNulls(t *testing.T) {
	clause := RangeClause{
		Identifier:   `field`,
		IncludeNulls: true,
		MinValue:     0,
		MaxValue:     10,
	}
	where := clause.ToSqlSnippet()
	expected := `("field" BETWEEN ? AND ? OR "field" IS NULL)`
	if where.Snippet != expected {
		t.Fatalf(`expected where clause of '%v' but got '%v'`, expected, where.Snippet)
	}
	t.Log(where.Snippet)
}

func TestWhereGreaterThanEqualMin(t *testing.T) {
	clause := RangeClause{
		Identifier:   `field`,
		IncludeNulls: false,
		MinValue:     0,
		MaxValue:     nil,
	}
	where := clause.ToSqlSnippet()
	expected := `"field" >= ?`
	if where.Snippet != expected {
		t.Fatalf(`expected where clause of '%v' but got '%v'`, expected, where.Snippet)
	}
	if where.Params[0] != 0 {
		t.Fatalf(`expected param of 0 but got %v`, where.Params[0])
	}
	t.Log(where.Snippet)
}

func TestWhereGreaterThanEqualMinIncludeNulls(t *testing.T) {
	clause := RangeClause{
		Identifier:   `field`,
		IncludeNulls: true,
		MinValue:     0,
		MaxValue:     nil,
	}
	where := clause.ToSqlSnippet()
	expected := `("field" >= ? OR "field" IS NULL)`
	if where.Snippet != expected {
		t.Fatalf(`expected where clause of '%v' but got '%v'`, expected, where.Snippet)
	}
	if where.Params[0] != 0 {
		t.Fatalf(`expected param of 0 but got %v`, where.Params[0])
	}
	t.Log(where.Snippet)
}

func TestWhereLessThanEqualMax(t *testing.T) {
	clause := RangeClause{
		Identifier:   `field`,
		IncludeNulls: false,
		MinValue:     nil,
		MaxValue:     10,
	}
	where := clause.ToSqlSnippet()
	expected := `"field" <= ?`
	if where.Snippet != expected {
		t.Fatalf(`expected where clause of '%v' but got '%v'`, expected, where.Snippet)
	}
	if where.Params[0] != 10 {
		t.Fatalf(`expected param of 10 but got %v`, where.Params[0])
	}
	t.Log(where.Snippet)
}

func TestWhereLessThanEqualMaxIncludeNulls(t *testing.T) {
	clause := RangeClause{
		Identifier:   `field`,
		IncludeNulls: true,
		MinValue:     nil,
		MaxValue:     10,
	}
	where := clause.ToSqlSnippet()
	expected := `("field" <= ? OR "field" IS NULL)`
	if where.Snippet != expected {
		t.Fatalf(`expected where clause of '%v' but got '%v'`, expected, where.Snippet)
	}
	if where.Params[0] != 10 {
		t.Fatalf(`expected param of 10 but got %v`, where.Params[0])
	}
	t.Log(where.Snippet)
}

func TestWhereClauseInWithZeroItems(t *testing.T) {
	wheres := []SqlSnippetGenerator{
		&InClause{
			Identifier: `field 1`,
			Exclude:    false,
			Values:     []interface{}{},
		},
	}
	where := GenerateCombinedWhereClause(wheres)
	expected := `1=2`
	if where.Value != expected {
		t.Fatalf(`expected '%v' but got '%v'`, expected, where.Value)
	}
	if len(where.Params) != 0 {
		t.Fatalf(`expected 0 params but got %v`, len(where.Params))
	}
}

func TestWhereClauseNotInWithZeroItems(t *testing.T) {
	wheres := []SqlSnippetGenerator{
		&InClause{
			Identifier: `field 1`,
			Exclude:    true,
			Values:     []interface{}{},
		},
	}
	where := GenerateCombinedWhereClause(wheres)
	expected := `NOT (1=2)`
	if where.Value != expected {
		t.Fatalf(`expected '%v' but got '%v'`, expected, where.Value)
	}
	if len(where.Params) != 0 {
		t.Fatalf(`expected 0 params but got %v`, len(where.Params))
	}
}
