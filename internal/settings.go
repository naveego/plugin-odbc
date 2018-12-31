package internal

import (
	"errors"
	"strings"
)

// Settings object for plugin
// Contains connection information and pre/post queries
type Settings struct {
	ConnectionString string `json:"connectionString"`
	Password         string `json:"password"`
	PrePublishQuery  string `json:"prePublishQuery"`
	PostPublishQuery string `json:"postPublishQuery"`
}

// Validate returns an error if the Settings are not valid.
// It also populates the internal fields of settings.
func (s *Settings) Validate() error {
	if s.ConnectionString == "" {
		return errors.New("the connectionString property must be set")
	}

	if s.Password == "" {
		return errors.New("the password property must be set")
	}

	return nil
}

// GetConnectionString builds a connection string from a settings object
func (s *Settings) GetConnectionString() (string, error) {
	out := strings.Replace(s.ConnectionString, "PASSWORD", s.Password, 1)
	return out, nil
}
