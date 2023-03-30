package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/BurntSushi/toml"
	"github.com/elektito/gcrawler/pkg/utils"
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

	Index struct {
		// the path in which we look for the index directories
		Path string
	}

	Capsule struct {
		// tls certificate and key files
		CertFile string
		KeyFile  string

		// bind port and bind ip address
		Port    int
		Address string
	}
}

const ConfigFilename = "gcrawler.toml"

var Config ConfigType

func init() {
	// set default values
	Config.Db.Name = "gcrawler"
	Config.Db.Port = -1
	Config.Db.Host = "/var/run/postgresql"

	Config.Index.Path = "."

	Config.Capsule.CertFile = "cert.pem"
	Config.Capsule.KeyFile = "key.pem"
	Config.Capsule.Port = 1965
	Config.Capsule.Address = "127.0.0.1"

	f, err := os.Open(ConfigFilename)
	if err != nil {
		fmt.Printf("Cannot open config file %s; Proceeding with defaults.\n", ConfigFilename)
		return
	}

	_, err = toml.DecodeReader(f, &Config)
	if err != nil {
		utils.PanicOnErr(err)
	}
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

func GetBindAddrAndPort() string {
	return Config.Capsule.Address + ":" + strconv.Itoa(Config.Capsule.Port)
}
