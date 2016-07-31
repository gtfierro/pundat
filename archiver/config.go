package archiver

import (
	"gopkg.in/gcfg.v1"
)

type BWConfig struct {
	Address    string
	Entityfile string
	Namespace  string
	ListenNS   []string
}

type ARConfig struct {
	PeriodicReport bool
	BlockExpiry    int
}

type MDConfig struct {
	Address string
}

type Config struct {
	Archiver ARConfig
	BOSSWAVE BWConfig
	Metadata MDConfig
}

func LoadConfig(filename string) *Config {
	var configuration Config
	err := gcfg.ReadFileInto(&configuration, filename)
	if err != nil {
		log.Errorf("No configuration file found at %v, so checking current directory for archiver.ini (%v)", filename, err)
	} else {
		return &configuration
	}
	err = gcfg.ReadFileInto(&configuration, "./archiver.ini")
	if err != nil {
		log.Fatal("Could not find configuration files ./archiver.ini. Try running durandal makeconf")
	} else {
		return &configuration
	}
	return &configuration
}
