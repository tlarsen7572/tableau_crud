package persistance_mssql

import (
	"ABB/tableau_crud/persistance"
	"database/sql"
	_ "github.com/denisenkom/go-mssqldb"
	"testing"
)

/*
  These tests require a config file called 'persistance_mssql_test_config.go'
  The config file specifies a test Database to use for the following tests.  An examples looks like this:

package persistance_mssql

import "net/url"

var ServerUrl = &url.URL{
	Scheme:     "sqlserver",
	User:       url.UserPassword(`Username`,`Password`),
	Host:       "Server:Port",
}

var Server = MssqlPersistor{
	Server:   `IP Address`,
	Port:     "Port number",
	Username: "Username",
	Password: "Password",
	Database: "Database",
	Schema:   "dbo",
	Table:    "Table",
}

*/

func TestSelect(t *testing.T) {
	db, err := sql.Open(`sqlserver`, ServerUrl.String())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()

	rows, err := db.Query(`SELECT id, category, amount FROM TEST.dbo.tableau_extension_test`)
	if err != nil {
		t.Fatal(err.Error())
	}

	defer rows.Close()
	for rows.Next() {
		var id int
		var category string
		var amount interface{}
		if err := rows.Scan(&id, &category, &amount); err != nil {
			t.Fatal(err.Error())
		}
		t.Logf(`id: %v, category: '%v', amount: %v`, id, category, amount)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err.Error())
	}
}

func TestInsert(t *testing.T) {
	db, err := sql.Open(`sqlserver`, ServerUrl.String())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()

	result, err := db.Exec(`INSERT INTO TEST.dbo.tableau_extension_test (category, amount) VALUES ('new', 442.01)`)
	if err != nil {
		t.Fatalf(`error inserting: '%v'`, err.Error())
	}

	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Logf(`rows affected: %v`, affected)
}

func TestDelete(t *testing.T) {
	db, err := sql.Open(`sqlserver`, ServerUrl.String())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()

	result, err := db.Exec(`DELETE FROM TEST.dbo.tableau_extension_test WHERE category != 'blah'`)
	if err != nil {
		t.Fatalf(`error deleting: '%v'`, err.Error())
	}

	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Logf(`rows affected: %v`, affected)
}

func TestQuoteIdentifiers(t *testing.T) {
	identifiers := []string{"Field1", "Field 2", "Field []3"}
	quoted := QuoteIdentifiers(identifiers)
	expected := `[Field1],[Field 2],[Field []]3]`
	if quoted != `[Field1],[Field 2],[Field []]3]` {
		t.Fatalf(`expected '%v' but got '%v'`, expected, quoted)
	}
	t.Log(quoted)
}

func TestValidateCorrectParam(t *testing.T) {
	param := `param1`
	err := ValidateParam(param)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
}

func TestValidateParamWithoutLeadingLetter(t *testing.T) {
	param := `1param1`
	err := ValidateParam(param)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Log(err.Error())
}

func TestValidateParamWithInvalidCharacter(t *testing.T) {
	param := `param_1`
	err := ValidateParam(param)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Log(err.Error())
}

func TestWhereEqual(t *testing.T) {
	clause := EqualClause{Identifier: `field`, Value: `Value`}
	where, err := clause.ToSqlSnippet(`param`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `[field]=@param`
	if where.Snippet != expected {
		t.Fatalf(`expected "%v" but got "%v"`, expected, where)
	}
	if len(where.Params) != 1 {
		t.Fatalf(`expected 1 param but got %v`, len(where.Params))
	}
	if name := where.Params[0].Name; name != `param` {
		t.Fatalf(`expected 'param' name but got '%v'`, name)
	}
	if value := where.Params[0].Value; value != `Value` {
		t.Fatalf(`expected 'Value' value but got '%v'`, value)
	}
	t.Log(where.Snippet)
}

func TestWhereEqualWrongNumberOfParams(t *testing.T) {
	clause := EqualClause{Identifier: `field`, Value: `Value`}
	_, err := clause.ToSqlSnippet()
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Log(err.Error())
}

func TestWhereIn(t *testing.T) {
	clause := InClause{
		Identifier: `field`,
		Values: []interface{}{
			`value1`,
			`value2`,
		},
	}
	where, err := clause.ToSqlSnippet(`param1`, `param2`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `[field] IN (@param1,@param2)`
	if where.Snippet != expected {
		t.Fatalf(`expected where clause of '%v' but got '%v'`, expected, where.Snippet)
	}
	if count := len(where.Params); count != 2 {
		t.Fatalf(`expected 2 params but got %v`, count)
	}
	param := where.Params[0]
	if param.Name != `param1` {
		t.Fatalf(`expected name 'param1' but got '%v'`, param.Name)
	}
	if param.Value != `value1` {
		t.Fatalf(`expected value 'value1' but got '%v'`, param.Value)
	}
	param = where.Params[1]
	if param.Name != `param2` {
		t.Fatalf(`expected name 'param2' but got '%v'`, param.Name)
	}
	if param.Value != `value2` {
		t.Fatalf(`expected value 'value2' but got '%v'`, param.Value)
	}
	t.Log(where.Snippet)
}

func TestWhereInWrongNumberOfParams(t *testing.T) {
	clause := InClause{
		Identifier: `field`,
		Values: []interface{}{
			`value1`,
			`value2`,
		},
	}
	_, err := clause.ToSqlSnippet(`@param1`)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Log(err.Error())
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
	where, err := clause.ToSqlSnippet(`param1`, `param2`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `NOT ([field] IN (@param1,@param2))`
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
	where, err := clause.ToSqlSnippet(`param1`, `param2`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `[field] BETWEEN @param1 AND @param2`
	if where.Snippet != expected {
		t.Fatalf(`expected where clause of '%v' but got '%v'`, expected, where.Snippet)
	}
	if count := len(where.Params); count != 2 {
		t.Fatalf(`expected 2 params but got %v`, count)
	}
	param := where.Params[0]
	if param.Name != `param1` {
		t.Fatalf(`expected name 'param1' but got '%v'`, param.Name)
	}
	if param.Value != 0 {
		t.Fatalf(`expected value 0 but got '%v'`, param.Value)
	}
	param = where.Params[1]
	if param.Name != `param2` {
		t.Fatalf(`expected name 'param2' but got '%v'`, param.Name)
	}
	if param.Value != 10 {
		t.Fatalf(`expected value 10 but got '%v'`, param.Value)
	}
	t.Log(where.Snippet)
}

func TestWhereRangeWrongNumberOfParams(t *testing.T) {
	clause := InClause{
		Identifier: `field`,
		Values: []interface{}{
			0,
			10,
		},
	}
	_, err := clause.ToSqlSnippet(`@param1`)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Log(err.Error())
}

func TestCombineWhereClauses(t *testing.T) {
	clauses := []persistance.SqlSnippetGenerator{
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
	where, err := GenerateCombinedWhereClause(clauses, 1)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `[field1]=@param1 AND [field2] IN (@param2,@param3,@param4) AND [field3] BETWEEN @param5 AND @param6`
	if where.Value != expected {
		t.Fatalf(`expected '%v' but got '%v'`, expected, where)
	}
	if where.NextParam != 7 {
		t.Fatalf(`expected last param of 7 but got %v`, where.NextParam)
	}
	t.Logf(where.Value)
}

func TestUpdateFieldClauses(t *testing.T) {
	var clause = UpdateClause{
		Identifier: "field",
		NewValue:   123.0,
	}
	update, err := clause.ToSqlSnippet(`param1`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `[field]=@param1`
	if update.Snippet != expected {
		t.Fatalf(`expected '%v' but got '%v'`, expected, update.Snippet)
	}
	t.Log(update.Snippet)
}

func TestUpdateFieldWrongNumberOfParameters(t *testing.T) {
	clause := UpdateClause{
		Identifier: "field1",
		NewValue:   `123`,
	}
	_, err := clause.ToSqlSnippet(`param1`, `param2`)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}

func TestCombineUpdateClauses(t *testing.T) {
	clauses := []persistance.SqlSnippetGenerator{
		&UpdateClause{
			Identifier: "field1",
			NewValue:   10,
		},
		&UpdateClause{
			Identifier: "field2",
			NewValue:   `123`,
		},
	}
	update, err := GenerateCombinedUpdateClause(clauses, 1)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `[field1]=@param1,[field2]=@param2`
	if update.Value != expected {
		t.Fatalf(`expected '%v' but got '%v'`, expected, update.Value)
	}
	if update.NextParam != 3 {
		t.Fatalf(`expected next param to be 3 but got %v`, update.NextParam)
	}
	t.Logf(update.Value)
}

func TestFieldListClause(t *testing.T) {
	clause := FieldListClause{Fields: []string{`field1`, `field2`, `field3`}}
	fieldList, err := clause.ToSqlSnippet()
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `[field1],[field2],[field3]`
	if fieldList.Snippet != expected {
		t.Fatalf(`expected '%v' but got '%v'`, expected, fieldList.Snippet)
	}
	t.Logf(fieldList.Snippet)
}

func TestFieldListWrongNumberOfParameters(t *testing.T) {
	clause := FieldListClause{
		Fields: []string{`Field1`, `Field2`},
	}
	_, err := clause.ToSqlSnippet(`param1`)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}

func TestParamListClause(t *testing.T) {
	clause := ParamListClause{ParamValues: []interface{}{10, `123`}}
	paramList, err := clause.ToSqlSnippet(`param1`, `param2`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `@param1,@param2`
	if paramList.Snippet != expected {
		t.Fatalf(`expected '%v' but got '%v'`, expected, paramList.Snippet)
	}
	if count := len(paramList.Params); count != 2 {
		t.Fatalf(`expected 2 params but got %v`, count)
	}
	param := paramList.Params[0]
	if param.Name != `param1` {
		t.Fatalf(`expected name 'param1' but got '%v'`, param.Name)
	}
	if param.Value != 10 {
		t.Fatalf(`expected value 10 but got %v`, param.Value)
	}
	param = paramList.Params[1]
	if param.Name != `param2` {
		t.Fatalf(`expected name 'param2' but got '%v'`, param.Name)
	}
	if param.Value != `123` {
		t.Fatalf(`expected value '123' but got %v`, param.Value)
	}
	t.Logf(paramList.Snippet)
}

func TestParamListWrongNumberOfParameters(t *testing.T) {
	clause := ParamListClause{
		ParamValues: []interface{}{10, `123`},
	}
	_, err := clause.ToSqlSnippet(`param1`)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}

func TestInsertMssql(t *testing.T) {
	rowsAffected, err := Server.Insert(map[string]interface{}{
		`category`: `new`,
		`amount`:   21.34,
	})
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	if rowsAffected != 1 {
		t.Fatalf(`expected 1 row affected but got %v`, rowsAffected)
	}
}

func TestUpdateMssql(t *testing.T) {
	where := []persistance.SqlSnippetGenerator{
		&EqualClause{
			Identifier: `category`,
			Value:      `new`,
		},
		&EqualClause{
			Identifier: `amount`,
			Value:      21.34,
		},
	}
	values := []persistance.SqlSnippetGenerator{
		&UpdateClause{
			Identifier: "amount",
			NewValue:   43.12,
		},
	}
	rowsAffected, err := Server.Update(where, values)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	if rowsAffected != 1 {
		t.Fatalf(`expected 1 affected row but got %v`, rowsAffected)
	}
}

func TestDeleteMssql(t *testing.T) {
	where := []persistance.SqlSnippetGenerator{
		&EqualClause{
			Identifier: `category`,
			Value:      `new`,
		},
		&EqualClause{
			Identifier: `amount`,
			Value:      43.12,
		},
	}
	rowsAffected, err := Server.Delete(where)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	if rowsAffected != 1 {
		t.Fatalf(`expected 1 affected row but got %v`, rowsAffected)
	}
}

func TestReadAllMssql(t *testing.T) {
	fields := []string{`id`, `category`, `amount`}
	where := []persistance.SqlSnippetGenerator{}
	orderBy := []string{`id`}
	result, err := Server.Read(fields, where, orderBy, 10, 1)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	if result.RowCount == 0 {
		t.Fatalf(`expected at least 1 row but got 0`)
	}
	if result.TotalRowCount == 0 {
		t.Fatalf(`expected at least 1 total rows but got 0`)
	}
	t.Logf(`%v`, result.ColumnNames)
}

func TestReadMssql(t *testing.T) {
	fields := []string{`id`, `category`, `amount`}
	orderBy := []string{`id`}
	where := []persistance.SqlSnippetGenerator{
		&EqualClause{
			Identifier: "category",
			Value:      `blah`,
		},
	}
	result, err := Server.Read(fields, where, orderBy, 10, 1)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	if result.RowCount == 0 {
		t.Fatalf(`expected at least 1 row but got 0`)
	}
	t.Logf(`%v`, result.ColumnNames)
}

func TestReadMssqlWithZeroFields(t *testing.T) {
	fields := []string{}
	orderBy := []string{`id`}
	where := []persistance.SqlSnippetGenerator{
		&EqualClause{
			Identifier: "category",
			Value:      `blah`,
		},
	}
	_, err := Server.Read(fields, where, orderBy, 10, 1)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}
func TestReadMssqlWithZeroOrderBy(t *testing.T) {
	fields := []string{`id`, `category`, `amount`}
	orderBy := []string{}
	where := []persistance.SqlSnippetGenerator{
		&EqualClause{
			Identifier: "category",
			Value:      `blah`,
		},
	}
	_, err := Server.Read(fields, where, orderBy, 10, 1)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}

func TestTestConnection(t *testing.T) {
	result, err := Server.TestConnection()
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	if result.RowCount != 0 {
		t.Fatalf(`expected zero rows but got %v`, result.RowCount)
	}
	if result.TotalRowCount != 0 {
		t.Fatalf(`expected zero total rows but got %v`, result.TotalRowCount)
	}
}

func TestWhereRangeIncludeNulls(t *testing.T) {
	clause := RangeClause{
		Identifier:   `field`,
		IncludeNulls: true,
		MinValue:     0,
		MaxValue:     10,
	}
	where, err := clause.ToSqlSnippet(`param1`, `param2`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `([field] BETWEEN @param1 AND @param2 OR [field] IS NULL)`
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
	where, err := clause.ToSqlSnippet(`param1`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `[field] >= @param1`
	if where.Snippet != expected {
		t.Fatalf(`expected where clause of '%v' but got '%v'`, expected, where.Snippet)
	}
	if where.Params[0].Value != 0 {
		t.Fatalf(`expected param of 0 but got %v`, where.Params[0].Value)
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
	where, err := clause.ToSqlSnippet(`param1`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `([field] >= @param1 OR [field] IS NULL)`
	if where.Snippet != expected {
		t.Fatalf(`expected where clause of '%v' but got '%v'`, expected, where.Snippet)
	}
	if where.Params[0].Value != 0 {
		t.Fatalf(`expected param of 0 but got %v`, where.Params[0].Value)
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
	where, err := clause.ToSqlSnippet(`param1`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `[field] <= @param1`
	if where.Snippet != expected {
		t.Fatalf(`expected where clause of '%v' but got '%v'`, expected, where.Snippet)
	}
	if where.Params[0].Value != 10 {
		t.Fatalf(`expected param of 10 but got %v`, where.Params[0].Value)
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
	where, err := clause.ToSqlSnippet(`param1`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `([field] <= @param1 OR [field] IS NULL)`
	if where.Snippet != expected {
		t.Fatalf(`expected where clause of '%v' but got '%v'`, expected, where.Snippet)
	}
	if where.Params[0].Value != 10 {
		t.Fatalf(`expected param of 10 but got %v`, where.Params[0].Value)
	}
	t.Log(where.Snippet)
}

func TestMultipleSQL(t *testing.T) {
	db, _ := sql.Open(`sqlserver`, Server.generateUrl())
	query := `SELECT * FROM TEST.dbo.tableau_extension_test; SELECT count(*) FROM TEST.dbo.tableau_extension_test`
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf(err.Error())
	}
	for rows.Next() {
		colNames, err := rows.Columns()
		if err != nil {
			t.Fatalf(err.Error())
		}
		rowValues := make([]interface{}, len(colNames))
		rowPointers := make([]interface{}, len(colNames))
		for index := range colNames {
			rowPointers[index] = &rowValues[index]
		}
		err = rows.Scan(rowPointers...)
		if err != nil {
			t.Fatalf(err.Error())
		}
		t.Logf(`%v`, rowValues)
	}
	if rows.NextResultSet() {
		for rows.Next() {
			colNames, err := rows.Columns()
			if err != nil {
				t.Fatalf(err.Error())
			}
			rowValues := make([]interface{}, len(colNames))
			rowPointers := make([]interface{}, len(colNames))
			for index := range colNames {
				rowPointers[index] = &rowValues[index]
			}
			err = rows.Scan(rowPointers...)
			if err != nil {
				t.Fatalf(err.Error())
			}
			t.Logf(`%v`, rowValues)
		}
	}
}

func TestWhereClauseInWithZeroItems(t *testing.T) {
	wheres := []persistance.SqlSnippetGenerator{
		&InClause{
			Identifier: `field 1`,
			Exclude:    false,
			Values:     []interface{}{},
		},
	}
	where, err := GenerateCombinedWhereClause(wheres, 1)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `1=2`
	if where.Value != expected {
		t.Fatalf(`expected '%v' but got '%v'`, expected, where.Value)
	}
	if len(where.Params) != 0 {
		t.Fatalf(`expected 0 params but got %v`, len(where.Params))
	}
}

func TestWhereClauseNotInWithZeroItems(t *testing.T) {
	wheres := []persistance.SqlSnippetGenerator{
		&InClause{
			Identifier: `field 1`,
			Exclude:    true,
			Values:     []interface{}{},
		},
	}
	where, err := GenerateCombinedWhereClause(wheres, 1)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `NOT (1=2)`
	if where.Value != expected {
		t.Fatalf(`expected '%v' but got '%v'`, expected, where.Value)
	}
	if len(where.Params) != 0 {
		t.Fatalf(`expected 0 params but got %v`, len(where.Params))
	}
}
