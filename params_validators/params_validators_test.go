package params_validators

import (
	"encoding/json"
	"testing"
)

func TestValidateInsert(t *testing.T) {
	params := map[string]interface{}{
		`values`: map[string]interface{}{
			`field1`: 123,
			`field2`: `some text`,
		},
	}
	values, err := ValidateInsertParams(params)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	value, ok := values[`field1`]
	if !ok {
		t.Fatalf(`missing field1 entry`)
	}
	if value != 123 {
		t.Fatalf(`expected field1 to be 123 but got %v`, value)
	}
	value, ok = values[`field2`]
	if !ok {
		t.Fatalf(`missing field2 entry`)
	}
	if value != `some text` {
		t.Fatalf(`expected field2 to be 'some text' but got '%v'`, value)
	}
}

func TestValidateInsertInvalidParams(t *testing.T) {
	params := map[string]interface{}{
		`values`: 123,
	}
	_, err := ValidateInsertParams(params)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}

func TestValidateInsertMissingParams(t *testing.T) {
	params := map[string]interface{}{
		`something`: `hello world`,
	}
	_, err := ValidateInsertParams(params)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}

func TestValidateInsertFromJson(t *testing.T) {
	jsonStr := `{"values":{"field1": 123, "field2": "blah"}}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	params, err := ValidateInsertParams(mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	t.Logf(`%v`, params)
}

func TestValidateUpdate(t *testing.T) {
	params := map[string]interface{}{
		`where`: []interface{}{
			map[string]interface{}{`field`: `field1`, `operator`: `equals`, `values`: []interface{}{123}},
			map[string]interface{}{`field`: `field2`, `operator`: `in`, `values`: []interface{}{`A`, `B`}},
			map[string]interface{}{`field`: `field3`, `operator`: `range`, `values`: []interface{}{0, 10}},
		},
		`updates`: map[string]interface{}{
			`field1`: 123,
			`field2`: `some text`,
		},
	}
	updateParams, err := ValidateUpdateParams(params)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	if len(updateParams.Updates) != 2 {
		t.Fatalf(`expected 2 updates but got %v`, len(updateParams.Updates))
	}
	if len(updateParams.Where) != 3 {
		t.Fatalf(`expected 3 Where clauses but got %v`, len(updateParams.Where))
	}
}

func TestValidateUpdateFromJson(t *testing.T) {
	jsonStr := `{"where":[{"field": "field1", "operator": "equals", "exclude": false, "includeNulls": false, "values": [10]}], "updates": {"field1": 20}}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	params, err := ValidateUpdateParams(mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	t.Logf(`%v`, params)
}

func TestValidateUpdateMissingWhereEqual(t *testing.T) {
	jsonStr := `{"updates": {"field1": 20}}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	_, err = ValidateUpdateParams(mapped)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}

func TestValidateUpdateMissingUpdates(t *testing.T) {
	jsonStr := `{"where":[{"field": "field1", "operator": "equals", "exclude": false, "includeNulls": false, "values": [10]}]}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	_, err = ValidateUpdateParams(mapped)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}

func TestValidateUpdateWhereMissingField(t *testing.T) {
	jsonStr := `{"where":[{"operator": "equals", "exclude": false, "includeNulls": false, "values": [10]}], "updates": {"field1": 20}}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	_, err = ValidateUpdateParams(mapped)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}

func TestValidateUpdateWhereMissingOperator(t *testing.T) {
	jsonStr := `{"where":[{"field": "field1", "exclude": false, "includeNulls": false, "values": [10]}], "updates": {"field1": 20}}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	_, err = ValidateUpdateParams(mapped)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}

func TestValidateUpdateWhereInvalidOperator(t *testing.T) {
	jsonStr := `{"where":[{"field": "field1", "operator":"invalid", "exclude": false, "includeNulls": false, "values": [10]}], "updates": {"field1": 20}}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	_, err = ValidateUpdateParams(mapped)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}

func TestValidateUpdateWhereMissingValues(t *testing.T) {
	jsonStr := `{"where":[{"field": "field1", "operator":"equals", "exclude": false, "includeNulls": false}], "updates": {"field1": 20}}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	_, err = ValidateUpdateParams(mapped)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}

func TestValidateUpdateWhereEqualsTwoValues(t *testing.T) {
	jsonStr := `{"where":[{"field": "field1", "operator":"equals", "exclude": false, "includeNulls": false, "values": [10,20]}], "updates": {"field1": 20}}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	_, err = ValidateUpdateParams(mapped)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}

func TestValidateUpdateWhereRangeNotTwoValues(t *testing.T) {
	jsonStr := `{"where":[{"field": "field1", "operator":"range", "exclude": false, "includeNulls": false, "values": [10,20,30]}], "updates": {"field1": 20}}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	_, err = ValidateUpdateParams(mapped)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}

func TestValidateUpdateWhereFieldNotString(t *testing.T) {
	jsonStr := `{"where":[{"field": 123, "operator":"range", "exclude": false, "includeNulls": false, "values": [10,30]}], "updates": {"field1": 20}}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	_, err = ValidateUpdateParams(mapped)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}

func TestValidateUpdateWhereOperatorNotString(t *testing.T) {
	jsonStr := `{"where":[{"field": "field1", "operator":2, "exclude": false, "includeNulls": false, "values": [10,30]}], "updates": {"field1": 20}}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	_, err = ValidateUpdateParams(mapped)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}

func TestValidateDelete(t *testing.T) {
	jsonStr := `{"where":[{"field": "field1", "operator":"equals", "exclude": false, "includeNulls": false, "values": [10]}]}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	deleteParams, err := ValidateDeleteParams(mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	if len(deleteParams) != 1 {
		t.Fatalf(`expected 1 where clause but got %v`, len(deleteParams))
	}
}

func TestValidateRead(t *testing.T) {
	jsonStr := `{"fields":["field1","field2"], "where":[{"field": "field1", "operator":"equals", "exclude": false, "includeNulls": false, "values": [10]}], "orderBy":["field1"], "pageSize": 10, "page": 1}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	readParams, err := ValidateReadParams(mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	if len(readParams.Fields) != 2 {
		t.Fatalf(`expected 2 fields but got %v`, len(readParams.Fields))
	}
	if len(readParams.Where) != 1 {
		t.Fatalf(`expected 1 where clause but got %v`, len(readParams.Where))
	}
	if len(readParams.OrderBy) != 1 {
		t.Fatalf(`expected 1 order by but got %v`, len(readParams.OrderBy))
	}
	if readParams.Page != 1 {
		t.Fatalf(`expected page 1 but got %v`, readParams.Page)
	}
	if readParams.PageSize != 10 {
		t.Fatalf(`expected page size 10 but got %v`, readParams.PageSize)
	}
}

func TestValidateReadWhereNotIn(t *testing.T) {
	jsonStr := `{"fields":["field1","field2"], "where":[{"field": "field1", "operator":"in", "exclude": true, "includeNulls": false, "values": [10]}], "orderBy":["field1"], "pageSize": 10, "page": 1}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	readParams, err := ValidateReadParams(mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	snippet, err := readParams.Where[0].ToSqlSnippet(`param1`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `NOT ([field1] IN (@param1))`
	if snippet.Snippet != expected {
		t.Fatalf(`expected '%v' but got '%v'`, expected, snippet.Snippet)
	}
}

func TestValidateReadWhereGreaterThanEqual(t *testing.T) {
	jsonStr := `{"fields":["field1","field2"], "where":[{"field": "field1", "operator":"range", "exclude": false, "includeNulls": false, "values": [0,null]}], "orderBy":["field1"], "pageSize": 10, "page": 1}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	readParams, err := ValidateReadParams(mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	snippet, err := readParams.Where[0].ToSqlSnippet(`param1`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `[field1] >= @param1`
	if snippet.Snippet != expected {
		t.Fatalf(`expected '%v' but got '%v'`, expected, snippet.Snippet)
	}
}

func TestValidateReadWhereLessThanEqual(t *testing.T) {
	jsonStr := `{"fields":["field1","field2"], "where":[{"field": "field1", "operator":"range", "exclude": false, "includeNulls": false, "values": [null,10]}], "orderBy":["field1"], "pageSize": 10, "page": 1}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	readParams, err := ValidateReadParams(mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	snippet, err := readParams.Where[0].ToSqlSnippet(`param1`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `[field1] <= @param1`
	if snippet.Snippet != expected {
		t.Fatalf(`expected '%v' but got '%v'`, expected, snippet.Snippet)
	}
}

func TestValidateReadWhereRangeIncludeNulls(t *testing.T) {
	jsonStr := `{"fields":["field1","field2"], "where":[{"field": "field1", "operator":"range", "exclude": false, "includeNulls": true, "values": [0,10]}], "orderBy":["field1"], "pageSize": 10, "page": 1}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	readParams, err := ValidateReadParams(mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	snippet, err := readParams.Where[0].ToSqlSnippet(`param1`, `param2`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `([field1] BETWEEN @param1 AND @param2 OR [field1] IS NULL)`
	if snippet.Snippet != expected {
		t.Fatalf(`expected '%v' but got '%v'`, expected, snippet.Snippet)
	}
}

func TestValidateReadWhereGreaterThanEqualIncludeNulls(t *testing.T) {
	jsonStr := `{"fields":["field1","field2"], "where":[{"field": "field1", "operator":"range", "exclude": false, "includeNulls": true, "values": [0,null]}], "orderBy":["field1"], "pageSize": 10, "page": 1}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	readParams, err := ValidateReadParams(mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	snippet, err := readParams.Where[0].ToSqlSnippet(`param1`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `([field1] >= @param1 OR [field1] IS NULL)`
	if snippet.Snippet != expected {
		t.Fatalf(`expected '%v' but got '%v'`, expected, snippet.Snippet)
	}
}

func TestValidateReadWhereLessThanEqualIncludeNulls(t *testing.T) {
	jsonStr := `{"fields":["field1","field2"], "where":[{"field": "field1", "operator":"range", "exclude": false, "includeNulls": true, "values": [null,10]}], "orderBy":["field1"], "pageSize": 10, "page": 1}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	readParams, err := ValidateReadParams(mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	snippet, err := readParams.Where[0].ToSqlSnippet(`param1`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	expected := `([field1] <= @param1 OR [field1] IS NULL)`
	if snippet.Snippet != expected {
		t.Fatalf(`expected '%v' but got '%v'`, expected, snippet.Snippet)
	}
}

func TestValidateEncryptPassword(t *testing.T) {
	jsonStr := `{"password":"blah blah blah"}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	encryptParam, err := ValidateEncryptPasswordParams(mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	if encryptParam.Password != `blah blah blah` {
		t.Fatalf(`expected 'blah blah blah' but got '%v'`, encryptParam.Password)
	}
}

func TestValidateEncryptPasswordWithoutParam(t *testing.T) {
	jsonStr := `{}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	_, err = ValidateEncryptPasswordParams(mapped)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}

func TestValidateEncryptPasswordInvalidParam(t *testing.T) {
	jsonStr := `{"password":12345}`
	var mapped map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapped)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	_, err = ValidateEncryptPasswordParams(mapped)
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}

func TestEncryptAndDecrypt(t *testing.T) {
	password := `12345`
	encrypted, err := Encrypt(password)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	decrypted, err := Decrypt(encrypted)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	if decrypted != password {
		t.Fatalf(`expected decrypted to be '%v' but got '%v'`, password, decrypted)
	}
}
