package server

import (
	_ "github.com/snowflakedb/gosnowflake"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestConnList(t *testing.T) {
	s, err := LoadServer(`validSettings.json`)
	if err != nil {
		t.Fatalf(`got error %v`, err.Error())
	}
	body := io.NopCloser(strings.NewReader(`{"ApiKey":"12345"}`))
	w := httptest.NewRecorder()
	r := httptest.NewRequest(`POST`, `https://test.com/api/connections`, body)
	s.Handler.ServeHTTP(w, r)

	t.Logf(w.Body.String())
	if w.Code != 200 {
		t.Fatalf(`expected 200 but got %v`, w.Code)
	}
}

func TestSelect(t *testing.T) {
	s, err := LoadServer(`validSettings.json`)
	if err != nil {
		t.Fatalf(`got error %v`, err.Error())
	}
	body := io.NopCloser(strings.NewReader(`{"ApiKey":"12345","Connection":"test","Table":"TABLEAU_CRUD_TEST","Fields":["KEY","NAME","AT"],"OrderBy":["KEY"],"PageSize":10,"Page":1}`))
	w := httptest.NewRecorder()
	r := httptest.NewRequest(`POST`, `https://test.com/api/select`, body)
	s.Handler.ServeHTTP(w, r)

	t.Logf(w.Body.String())
	if w.Code != 200 {
		t.Fatalf(`expected 200 but got %v`, w.Code)
	}
}

func TestTest(t *testing.T) {
	s, err := LoadServer(`validSettings.json`)
	if err != nil {
		t.Fatalf(`got error %v`, err.Error())
	}
	body := io.NopCloser(strings.NewReader(`{"ApiKey":"12345","Connection":"test","Table":"TABLEAU_CRUD_TEST"}`))
	w := httptest.NewRecorder()
	r := httptest.NewRequest(`POST`, `https://test.com/api/test`, body)
	s.Handler.ServeHTTP(w, r)

	t.Logf(w.Body.String())
	if w.Code != 200 {
		t.Fatalf(`expected 200 but got %v`, w.Code)
	}

}

func TestInsertUpdateDelete(t *testing.T) {
	s, err := LoadServer(`validSettings.json`)
	if err != nil {
		t.Fatalf(`got error %v`, err.Error())
	}
	body := io.NopCloser(strings.NewReader(`{"ApiKey":"12345","Connection":"test","Table":"TABLEAU_CRUD_TEST","Values":{"KEY":3,"NAME":"Test New Record","AT":"2023-01-02T03:04:05"}}`))
	w := httptest.NewRecorder()
	r := httptest.NewRequest(`POST`, `https://test.com/api/insert`, body)
	s.Handler.ServeHTTP(w, r)
	t.Logf(w.Body.String())
	if w.Code != 200 {
		t.Fatalf(`expected 200 but got %v`, w.Code)
	}

	body = io.NopCloser(strings.NewReader(`{"ApiKey":"12345","Connection":"test","Table":"TABLEAU_CRUD_TEST","WHERE":[{"field":"KEY","operator":"equals","values":[3]}],"Updates":{"NAME":"New Name"}}`))
	w = httptest.NewRecorder()
	r = httptest.NewRequest(`POST`, `https://test.com/api/update`, body)
	s.Handler.ServeHTTP(w, r)
	t.Logf(w.Body.String())
	if w.Code != 200 {
		t.Fatalf(`expected 200 but got %v`, w.Code)
	}

	body = io.NopCloser(strings.NewReader(`{"ApiKey":"12345","Connection":"test","Table":"TABLEAU_CRUD_TEST","WHERE":[{"field":"KEY","operator":"equals","values":[3]}]}`))
	w = httptest.NewRecorder()
	r = httptest.NewRequest(`POST`, `https://test.com/api/delete`, body)
	s.Handler.ServeHTTP(w, r)
	t.Logf(w.Body.String())
	if w.Code != 200 {
		t.Fatalf(`expected 200 but got %v`, w.Code)
	}
}
