package config

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/elektito/gemplex/pkg/utils"
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

		// batch size used when indexing; higher values increase indexing
		// performance, but also increase memory consumption.
		BatchSize int
	}

	Search struct {
		UnixSocketPath string
	}

	Args []string `toml:"-"`
}

const DefaultConfigFilename = "gemplex.toml"

var Config ConfigType
var ConfigFilename *string

func init() {
	ConfigFilename = flag.String("config", "", "The config file to use.")
	flag.Usage = usage
	flag.Parse()
	Config.Args = flag.Args()

	usingDefaultConfigFile := false
	if *ConfigFilename == "" {
		*ConfigFilename = DefaultConfigFilename
		usingDefaultConfigFile = true
	}

	// set default values
	Config.Db.Name = "gemplex"
	Config.Db.Port = -1
	Config.Db.Host = "/var/run/postgresql"

	Config.Index.Path = "."
	Config.Index.BatchSize = 200

	Config.Search.UnixSocketPath = "/tmp/gsearch.sock"

	f, err := os.Open(*ConfigFilename)
	if err != nil && usingDefaultConfigFile {
		log.Printf("Cannot open default config file %s; Proceeding with defaults.\n", *ConfigFilename)
		return
	} else if err != nil {
		log.Fatal("Cannot open config file: ", *ConfigFilename)
	}

	_, err = toml.DecodeReader(f, &Config)
	if err != nil {
		utils.PanicOnErr(err)
	}
}

func usage() {
	fmt.Printf(`Gemplex Search Engine

usage: %s [-config config_file] { all | <commands> }

config_file is the name of the toml configuration file to load. If not
specified, %s is used.

<commands> can be one or more of these commands, separated by spaces. If "all"
is used, all daemons are launched.

 - crawl: Start the crawler daemon. The crawler routinely crawls the geminispace
   and stores the results in the database.

 - rank: Start the periodic pagerank calculator damon.

 - index: Start the periodic ping-pong indexer daemon. It builds, alternatingly,
   an index named "ping" or "pong".

 - search: Start the search daemon, which opens the latest index (either ping or
   pong), and listens for search requests over a unix domain socket.

`, DefaultConfigFilename, os.Args[0])
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
