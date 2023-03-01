package main

import (
	"fmt"
	_ "github.com/snowflakedb/gosnowflake"
	"net/http"
	"path/filepath"
	"tableau_crud/server"
)

func main() {
	println(`loading server...`)
	s, err := server.LoadServer(filepath.Join(`.`, `server.json`))
	if err != nil {
		println(err.Error())
		return
	}

	if s.Settings.UseTls {
		println(fmt.Sprintf(`listening on %v with SSL`, s.Settings.Address))
		err = http.ListenAndServeTLS(
			s.Settings.Address,
			filepath.Join(`.`, `cert.pem`),
			filepath.Join(`.`, `key.pem`),
			s.Handler,
		)
	} else {
		println(fmt.Sprintf(`listening on %v`, s.Settings.Address))
		err = http.ListenAndServe(s.Settings.Address, s.Handler)
	}
}
