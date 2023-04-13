package config

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/elektito/gemplex/pkg/utils"
)

type Config struct {
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

		// batch size used when indexing; higher values increase indexing
		// performance, but also increase memory consumption.
		BatchSize int
	}

	Search struct {
		UnixSocketPath string
	}
}

var DefaultConfigFiles = []string{"gemplex.toml", "/etc/gemplex.toml"}

func LoadConfig(configFilename string) *Config {
	c := new(Config)

	// set default values
	c.Db.Name = "gemplex"
	c.Db.Port = -1
	c.Db.Host = "/var/run/postgresql"

	c.Index.Path = "."
	c.Index.BatchSize = 200

	c.Search.UnixSocketPath = "/tmp/gsearch.sock"

	var f *os.File
	var err error
	if configFilename != "" {
		f, err = os.Open(configFilename)
	} else {
		for _, filename := range DefaultConfigFiles {
			f, err = os.Open(filename)
			if err == nil {
				configFilename = filename
				break
			}
		}
	}

	if err != nil {
		if configFilename != "" {
			log.Fatal("Cannot open config file: ", configFilename)
		} else {
			defaultFiles := strings.Join(DefaultConfigFiles, ", ")
			log.Printf("Cannot open any of the default config files (%s); Proceeding with defaults.\n", defaultFiles)
			return c
		}
	}

	log.Println("Using config file:", configFilename)

	_, err = toml.DecodeReader(f, c)
	if err != nil {
		utils.PanicOnErr(err)
	}
	return c
}

func (c *Config) GetDbConnStr() string {
	s := fmt.Sprintf(
		"dbname=%s sslmode=%t host=%s",
		c.Db.Name, c.Db.SslMode, c.Db.Host,
	)

	if c.Db.Port > 0 {
		s += fmt.Sprintf(" port=%d", c.Db.Port)
	}

	if c.Db.Password != "" {
		s += fmt.Sprintf(" password=%s", c.Db.Password)
	}

	return s
}
