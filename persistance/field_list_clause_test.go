package persistance

import (
	"testing"
)

func TestFieldListClause(t *testing.T) {
	clause := FieldListClause{Fields: []string{`field1`, `field2`, `field3`}}
	fieldList := clause.ToSqlSnippet()
	expected := `"field1","field2","field3"`
	if fieldList.Snippet != expected {
		t.Fatalf(`expected '%v' but got '%v'`, expected, fieldList.Snippet)
	}
	t.Logf(fieldList.Snippet)
}
