package persistance

import "testing"

func TestQuoteIdentifiers(t *testing.T) {
	identifiers := []string{"Field1", "Field 2", `Field "3"`}
	quoted := QuoteIdentifiers(identifiers)
	expected := `"Field1","Field 2","Field ""3"""`
	if quoted != expected {
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
