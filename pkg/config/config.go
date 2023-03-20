package config

import (
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
)

type ConfigType struct {
	Db struct {
		Name     string
		Host     string
		Port     int
		User     string
		Password string
		SslMode  bool
	}
}

const ConfigFilename = "gcrawler.toml"

var Config ConfigType

func init() {
	// set default values
	Config.Db.Name = "gcrawler"
	Config.Db.Port = -1
	Config.Db.Host = "/var/run/postgresql"

	f, err := os.Open(ConfigFilename)
	if err != nil {
		fmt.Printf("Cannot open config file %s; Proceeding with defaults.\n", ConfigFilename)
		return
	}

	toml.DecodeReader(f, &Config)
}

func GetDbConnStr() string {
	s := fmt.Sprintf(
		"dbname=%s sslmode=%t host=%s",
		Config.Db.Name, Config.Db.SslMode, Config.Db.Host,
	)

	if Config.Db.Port > 0 {
		s += fmt.Sprintf(" port=%d", Config.Db.Port)
	}

	if Config.Db.Password != "" {
		s += fmt.Sprintf(" password=%s", Config.Db.Password)
	}

	return s
}
