package settings

import "testing"

func TestLoadValidSettings(t *testing.T) {
	settings, err := LoadSettings(`.\validSettings.json`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	if settings.Address != `localhost:35012` {
		t.Fatalf(`expected port 1433 but got %v`, settings.Address)
	}
	if !settings.UseTls {
		t.Fatalf(`expected UseTls to be true but was false`)
	}
}

func TestLoadSettingsWithWrongDataType(t *testing.T) {
	settings, err := LoadSettings(`.\settingsWithWrongDataTypes.json`)
	if err == nil {
		t.Logf(`Address: %v, UseTls: %v`, settings.Address, settings.UseTls)
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}

func TestLoadSettingsWithMissingElements(t *testing.T) {
	settings, err := LoadSettings(`.\settingsWithMissingElements.json`)
	if err == nil {
		t.Logf(`Address: %v, UseTls: %v`, settings.Address, settings.UseTls)
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}

func TestLoadSettingsThatDoNotExist(t *testing.T) {
	settings, err := LoadSettings(`.\totallyNotThere.json`)
	if err == nil {
		t.Logf(`Address: %v, UseTls: %v`, settings.Address, settings.UseTls)
		t.Fatalf(`expected an error but got none`)
	}
	t.Logf(err.Error())
}
