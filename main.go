package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	errors "tableau_crud/error_messaging"
	v "tableau_crud/params_validators"
	p "tableau_crud/persistance_mssql"
	s "tableau_crud/settings"
	"time"
)

func main() {
	println(`loading settings...`)
	settings, err := s.LoadSettings(filepath.Join(`.`, `settings.json`))
	if err != nil {
		println(err.Error())
		return
	}

	println(`creating log...`)
	log, err := os.Create(filepath.Join(`.`, `log.txt`))
	if err != nil {
		println(err.Error())
		return
	}
	http.HandleFunc(`/`, handleDefault)
	http.HandleFunc(`/encryptpassword`, handleEncryptPassword)
	http.HandleFunc("/main.dart.js", handleFile)
	http.HandleFunc("/main.dart.js.map", handleFile)
	http.HandleFunc("/manifest.json", handleFile)
	http.HandleFunc("/flutter_service_worker.js", handleFile)
	http.HandleFunc("/tableau.extensions.1.latest.min.js", handleFile)
	http.HandleFunc("/assets/", handleFile)
	if settings.UseTls {
		println(fmt.Sprintf(`listening on %v with SSL`, settings.Address))
		err = http.ListenAndServeTLS(
			settings.Address,
			filepath.Join(`.`, `cert.pem`),
			filepath.Join(`.`, `key.pem`),
			nil,
		)
	} else {
		println(fmt.Sprintf(`listening on %v`, settings.Address))
		err = http.ListenAndServe(settings.Address, nil)
	}

	if err != nil {
		writeLog(log, err.Error())
	}
	_ = log.Close()
}

func handleFile(w http.ResponseWriter, r *http.Request) {
	file := r.URL.Path
	ext := filepath.Ext(file)
	if ext == `.js` || ext == `.json` || ext == `.map` {
		setHeaders(w, "application/javascript")
	}
	if ext == `.deps` {
		setHeaders(w, "text/plain")
	}
	http.ServeFile(w, r, filepath.Join(`html`, file))
}

func writeLog(log *os.File, msg string) {
	timestamp := time.Now().Format(time.RFC3339)
	entry := fmt.Sprintf(`%v - %v`, timestamp, msg)
	_, _ = log.WriteString(entry)
}

func setHeaders(w http.ResponseWriter, contentType string) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Content-Type", contentType)
}

type RequestPayload struct {
	Server     string
	Port       string
	Username   string
	Password   string
	Database   string
	Schema     string
	Table      string
	Function   string
	Parameters map[string]interface{}
}

type ResponsePayload struct {
	Success bool
	Data    interface{}
}

func handleDefault(writer http.ResponseWriter, r *http.Request) {
	if r.Method == `GET` {
		http.ServeFile(writer, r, filepath.Join(`html`, `index.html`))
		return
	}

	decoder := json.NewDecoder(r.Body)
	request := &RequestPayload{}
	err := decoder.Decode(request)
	if err != nil {
		sendErrorResponse(writer, errors.GenerateErrorMessage(`error decoding payload`, err))
		return
	}

	function := strings.ToLower(request.Function)
	decryptedPassword, err := v.Decrypt(request.Password)
	if err != nil {
		sendErrorResponse(writer, errors.GenerateErrorMessage(`error decrypting password`, err))
		return
	}
	persistor := p.MssqlPersistor{
		Server:   request.Server,
		Port:     request.Port,
		Username: request.Username,
		Password: decryptedPassword,
		Database: request.Database,
		Schema:   request.Schema,
		Table:    request.Table,
	}
	if function == `insert` {
		handleInsert(writer, persistor, request.Parameters)
		return
	}
	if function == `update` {
		handleUpdate(writer, persistor, request.Parameters)
		return
	}
	if function == `delete` {
		handleDelete(writer, persistor, request.Parameters)
		return
	}
	if function == `read` {
		handleRead(writer, persistor, request.Parameters)
		return
	}
	if function == `testconnection` {
		handleTestConnection(writer, persistor)
		return
	}
	sendErrorResponse(writer, `Function not valid, should be Insert, Update, Delete, Read, or TestConnection`)
}

func handleEncryptPassword(writer http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	params := map[string]interface{}{}
	err := decoder.Decode(&params)
	if err != nil {
		sendErrorResponse(writer, errors.GenerateErrorMessage(`error decoding payload`, err))
		return
	}

	validated, err := v.ValidateEncryptPasswordParams(params)
	if err != nil {
		sendErrorResponse(writer, errors.GenerateErrorValidatingParams(err))
		return
	}
	encrypted, err := v.Encrypt(validated.Password)
	if err != nil {
		sendErrorResponse(writer, errors.GenerateErrorMessage(`error encrypting password`, err))
	}
	sendNormalResponse(writer, encrypted)
}

func handleInsert(writer http.ResponseWriter, persistor p.MssqlPersistor, params map[string]interface{}) {
	validated, err := v.ValidateInsertParams(params)
	if err != nil {
		sendErrorResponse(writer, errors.GenerateErrorValidatingParams(err))
		return
	}
	result, err := persistor.Insert(validated)
	if err != nil {
		sendErrorResponse(writer, errors.GenerateErrorMessage(`error inserting records`, err))
		return
	}
	sendNormalResponse(writer, result)
}

func handleUpdate(writer http.ResponseWriter, persistor p.MssqlPersistor, params map[string]interface{}) {
	validated, err := v.ValidateUpdateParams(params)
	if err != nil {
		sendErrorResponse(writer, errors.GenerateErrorValidatingParams(err))
		return
	}
	result, err := persistor.Update(validated.Where, validated.Updates)
	if err != nil {
		sendErrorResponse(writer, errors.GenerateErrorMessage(`error updating records`, err))
		return
	}
	sendNormalResponse(writer, result)
}

func handleDelete(writer http.ResponseWriter, persistor p.MssqlPersistor, params map[string]interface{}) {
	validated, err := v.ValidateDeleteParams(params)
	if err != nil {
		sendErrorResponse(writer, errors.GenerateErrorValidatingParams(err))
		return
	}
	result, err := persistor.Delete(validated)
	if err != nil {
		sendErrorResponse(writer, errors.GenerateErrorMessage(`error deleting records`, err))
		return
	}
	sendNormalResponse(writer, result)
}

func handleRead(writer http.ResponseWriter, persistor p.MssqlPersistor, params map[string]interface{}) {
	validated, err := v.ValidateReadParams(params)
	if err != nil {
		sendErrorResponse(writer, errors.GenerateErrorValidatingParams(err))
		return
	}
	result, err := persistor.Read(validated.Fields, validated.Where, validated.OrderBy, validated.PageSize, validated.Page)
	if err != nil {
		sendErrorResponse(writer, errors.GenerateErrorMessage(`error reading records`, err))
		return
	}
	sendNormalResponse(writer, result)
}

func handleTestConnection(writer http.ResponseWriter, persistor p.MssqlPersistor) {
	result, err := persistor.TestConnection()
	if err != nil {
		sendErrorResponse(writer, errors.GenerateErrorMessage(`error testing connection`, err))
		return
	}
	sendNormalResponse(writer, result)
}

func sendNormalResponse(w http.ResponseWriter, data interface{}) {
	setHeaders(w, "application/json")
	response := ResponsePayload{true, data}
	responseBytes, marshalErr := json.Marshal(response)
	if marshalErr != nil {
		buffer := bytes.NewBufferString(marshalErr.Error())
		w.Write(buffer.Bytes())
		return
	}
	w.Write(responseBytes)
}

func sendErrorResponse(w http.ResponseWriter, err string) {
	setHeaders(w, "application/json")
	response := ResponsePayload{false, err}
	responseBytes, marshalErr := json.Marshal(response)
	if marshalErr != nil {
		var buffer = bytes.NewBufferString(marshalErr.Error())
		w.Write(buffer.Bytes())
		return
	}
	w.Write(responseBytes)
}
