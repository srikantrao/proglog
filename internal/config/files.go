package config

import (
	"os"
	"path/filepath"
)

var (
	CAFile = ConfigFile("ca.pem")
	ServerCertFile = ConfigFile("server.pem")
	ServerKeyFile = ConfigFile("server-key.pem")
)

// Get the absolute path to the current filename
func ConfigFile(filename string) string {
	if dir := os.Getenv("CONFIG_DIR"); dir != "" {
		return filepath.Join(dir, filename)
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	return filepath.Join(homeDir, ".proglog", filename)
}