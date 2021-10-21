package settings

import (
	"encoding/json"
	"io/ioutil"
)

type Settings struct {
	Address string
	UseTls  bool
}

func LoadSettings(settingsPath string) (*Settings, error) {
	contentBytes, err := ioutil.ReadFile(settingsPath)
	if err != nil {
		return nil, err
	}
	settings := &Settings{}
	err = json.Unmarshal(contentBytes, settings)
	if err != nil {
		return nil, err
	}
	return settings, nil
}
