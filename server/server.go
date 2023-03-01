package server

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"mime"
	"net/http"
	"os"
	"path"
	"path/filepath"
	errors "tableau_crud/error_messaging"
	v "tableau_crud/params_validators"
	"tableau_crud/persistance"
)

type Settings struct {
	Address     string
	UseTls      bool
	Connections []Connection
	ApiKey      string
}

type Connection struct {
	Name    string
	Driver  string
	ConnStr string
}

func loadSettings(settingsPath string) (Settings, error) {
	settings := Settings{}
	contentBytes, err := os.ReadFile(settingsPath)
	if err != nil {
		return settings, err
	}
	err = json.Unmarshal(contentBytes, &settings)
	if err != nil {
		return settings, err
	}
	return settings, nil
}

func LoadServer(settingsPath string) (*Server, error) {
	var err error
	server := &Server{
		Persistors: make(map[string]persistance.Persistor),
	}
	server.Settings, err = loadSettings(settingsPath)
	if err != nil {
		return nil, err
	}
	for _, conn := range server.Settings.Connections {
		switch {
		case conn.Driver == `snowflake`:
			persistor, err := persistance.NewPersistor(conn.ConnStr)
			if err != nil {
				return nil, err
			}
			server.Persistors[conn.Name] = persistor
		default:
			fmt.Printf(`invalid driver %q, expected 'snowflake'`, conn.Driver)
		}
	}

	m := mux.NewRouter()
	api := m.PathPrefix(`/api`).Methods(`POST`).Subrouter()
	m.Path(`/`).Methods(`GET`).HandlerFunc(server.handleHomepage)
	m.PathPrefix(`/`).Methods(`GET`).HandlerFunc(server.handleFile)

	api.Path(`/select`).HandlerFunc(server.handleRead)
	api.Path(`/insert`).HandlerFunc(server.handleInsert)
	api.Path(`/update`).HandlerFunc(server.handleUpdate)
	api.Path(`/delete`).HandlerFunc(server.handleDelete)
	api.Path(`/test`).HandlerFunc(server.handleTestConnection)
	api.Path(`/connections`).HandlerFunc(server.handleListConnections)

	server.Handler = m

	return server, nil
}

type Server struct {
	Settings   Settings
	Handler    http.Handler
	Persistors map[string]persistance.Persistor
}

func (s *Server) handleHomepage(w http.ResponseWriter, _ *http.Request) {
	fullPath := path.Join(`html`, `index.html`)
	content, err := os.ReadFile(fullPath)
	if err != nil {
		s.handle404(w)
		return
	}
	w.Header().Add("Content-Type", `text/html`)
	_, _ = w.Write(content)
}

func (s *Server) handleFile(w http.ResponseWriter, r *http.Request) {
	fullPath := path.Join(`html`, r.URL.Path)
	content, err := os.ReadFile(fullPath)
	if err != nil {
		s.handle404(w)
		return
	}
	mimeType := mime.TypeByExtension(filepath.Ext(fullPath))
	w.Header().Add("Content-Type", mimeType)
	_, _ = w.Write(content)
}

func (s *Server) handle404(w http.ResponseWriter) {
	err404, _ := os.ReadFile(path.Join(`html`, `404.html`))
	w.WriteHeader(404)
	_, _ = w.Write(err404)
}

func (s *Server) handleInsert(w http.ResponseWriter, r *http.Request) {
	params, err := validatePayload[InsertParams](s, r)
	if err != nil {
		sendErrorResponse(w, err.Error())
		return
	}
	persistor, ok := s.Persistors[params.Connection]
	if !ok {
		sendErrorResponse(w, fmt.Sprintf(`connection %q is not valid`, params.Connection))
		return
	}
	result, err := persistor.Insert(params.Table, params.Values)
	if err != nil {
		sendErrorResponse(w, errors.GenerateErrorMessage(`error inserting records`, err))
		return
	}
	sendNormalResponse(w, result)
}

func (s *Server) handleUpdate(w http.ResponseWriter, r *http.Request) {
	params, err := validatePayload[UpdateParams](s, r)
	if err != nil {
		sendErrorResponse(w, err.Error())
		return
	}
	persistor, ok := s.Persistors[params.Connection]
	if !ok {
		sendErrorResponse(w, fmt.Sprintf(`connection %q is not valid`, params.Connection))
		return
	}
	whereClauses, err := v.ValidateWhereClauses(params.Where)
	if err != nil {
		sendErrorResponse(w, fmt.Sprintf(`error decoding where clauses: %v`, err.Error()))
		return
	}
	updateClauses, err := v.ValidateUpdateClauses(params.Updates)
	if err != nil {
		sendErrorResponse(w, fmt.Sprintf(`error decoding update clauses: %v`, err.Error()))
		return
	}
	result, err := persistor.Update(params.Table, whereClauses, updateClauses)
	if err != nil {
		sendErrorResponse(w, errors.GenerateErrorMessage(`error updating records`, err))
		return
	}
	sendNormalResponse(w, result)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	params, err := validatePayload[DeleteParams](s, r)
	if err != nil {
		sendErrorResponse(w, err.Error())
		return
	}
	persistor, ok := s.Persistors[params.Connection]
	if !ok {
		sendErrorResponse(w, fmt.Sprintf(`connection %q is not valid`, params.Connection))
		return
	}
	whereClauses, err := v.ValidateWhereClauses(params.Where)
	if err != nil {
		sendErrorResponse(w, fmt.Sprintf(`error decoding where clauses: %v`, err.Error()))
		return
	}
	result, err := persistor.Delete(params.Table, whereClauses)
	if err != nil {
		sendErrorResponse(w, errors.GenerateErrorMessage(`error deleting records`, err))
		return
	}
	sendNormalResponse(w, result)
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request) {
	params, err := validatePayload[ReadParams](s, r)
	if err != nil {
		sendErrorResponse(w, err.Error())
		return
	}
	persistor, ok := s.Persistors[params.Connection]
	if !ok {
		sendErrorResponse(w, fmt.Sprintf(`connection %q is not valid`, params.Connection))
		return
	}
	whereClauses, err := v.ValidateWhereClauses(params.Where)
	if err != nil {
		sendErrorResponse(w, fmt.Sprintf(`error decoding where clauses: %v`, err.Error()))
		return
	}
	data, err := persistor.Read(params.Table, params.Fields, whereClauses, params.OrderBy, params.PageSize, params.Page)
	if err != nil {
		sendErrorResponse(w, err.Error())
		return
	}
	sendNormalResponse(w, data)
}

func (s *Server) handleTestConnection(w http.ResponseWriter, r *http.Request) {
	params, err := validatePayload[TestParams](s, r)
	if err != nil {
		sendErrorResponse(w, err.Error())
		return
	}
	persistor, ok := s.Persistors[params.Connection]
	if !ok {
		sendErrorResponse(w, fmt.Sprintf(`connection %q is not valid`, params.Connection))
	}
	result, err := persistor.TestConnection(params.Table)
	if err != nil {
		sendErrorResponse(w, errors.GenerateErrorMessage(`error testing connection`, err))
		return
	}
	sendNormalResponse(w, result)
}

func (s *Server) handleListConnections(w http.ResponseWriter, r *http.Request) {
	_, err := validatePayload[ConnectionListParams](s, r)
	if err != nil {
		sendErrorResponse(w, err.Error())
		return
	}
	connections := make([]string, 0, len(s.Persistors))
	for key := range s.Persistors {
		connections = append(connections, key)
	}
	sendNormalResponse(w, connections)
}

func (s *Server) checkApiKey(apiKey string) error {
	if apiKey == s.Settings.ApiKey {
		return nil
	}
	return fmt.Errorf(`api key is invalid`)
}

func validatePayload[T ApiKeyPayload](s *Server, r *http.Request) (T, error) {
	var params T
	j := json.NewDecoder(r.Body)
	err := j.Decode(&params)
	if err != nil {
		return params, err
	}
	err = s.checkApiKey(params.GetApiKey())
	return params, err
}

func setHeaders(w http.ResponseWriter, contentType string) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Content-Type", contentType)
}

func sendNormalResponse(w http.ResponseWriter, data interface{}) {
	setHeaders(w, "application/json")
	responseBytes, marshalErr := json.Marshal(data)
	if marshalErr != nil {
		sendErrorResponse(w, marshalErr.Error())
		return
	}
	_, _ = w.Write(responseBytes)
}

func sendErrorResponse(w http.ResponseWriter, err string) {
	w.WriteHeader(500)
	_, _ = w.Write([]byte(err))
}

type ApiKeyPayload interface {
	GetApiKey() string
}

type ReadParams struct {
	ApiKey     string
	Connection string
	Table      string
	Fields     []string
	Where      []interface{}
	OrderBy    []string
	PageSize   int
	Page       int
}

func (p ReadParams) GetApiKey() string {
	return p.ApiKey
}

type UpdateParams struct {
	ApiKey     string
	Connection string
	Table      string
	Where      []interface{}
	Updates    map[string]interface{}
}

func (p UpdateParams) GetApiKey() string {
	return p.ApiKey
}

type DeleteParams struct {
	ApiKey     string
	Connection string
	Table      string
	Where      []interface{}
}

func (p DeleteParams) GetApiKey() string {
	return p.ApiKey
}

type TestParams struct {
	ApiKey     string
	Connection string
	Table      string
}

func (p TestParams) GetApiKey() string {
	return p.ApiKey
}

type InsertParams struct {
	ApiKey     string
	Connection string
	Table      string
	Values     map[string]interface{}
}

func (p InsertParams) GetApiKey() string {
	return p.ApiKey
}

type ConnectionListParams struct {
	ApiKey string
}

func (p ConnectionListParams) GetApiKey() string {
	return p.ApiKey
}
