package persistance

import (
	"testing"
)

func TestUpdateFieldClauses(t *testing.T) {
	var clause = UpdateClause{
		Identifier: "field",
		NewValue:   123.0,
	}
	update := clause.ToSqlSnippet()
	expected := `"field"=?`
	if update.Snippet != expected {
		t.Fatalf(`expected '%v' but got '%v'`, expected, update.Snippet)
	}
	t.Log(update.Snippet)
}

func TestCombineUpdateClauses(t *testing.T) {
	clauses := []SqlSnippetGenerator{
		&UpdateClause{
			Identifier: "field1",
			NewValue:   10,
		},
		&UpdateClause{
			Identifier: "field2",
			NewValue:   `123`,
		},
	}
	update := GenerateCombinedUpdateClause(clauses)
	expected := `"field1"=?,"field2"=?`
	if update.Value != expected {
		t.Fatalf(`expected '%v' but got '%v'`, expected, update.Value)
	}
	t.Logf(update.Value)
}
