package archiver

import (
	"gopkg.in/gcfg.v1"
)

type BWConfig struct {
	Address    string
	Entityfile string
	Namespace  string
	DeployNS   string
	ListenNS   []string
}

type ARConfig struct {
	PeriodicReport bool
	BlockExpiry    string
}

type MDConfig struct {
	Address          string
	CollectionPrefix string
}

type BTRDBConfig struct {
	Address string
}

type BenchmarkConfig struct {
	EnableCPUProfile   bool
	EnableMEMProfile   bool
	EnableBlockProfile bool
}

type Config struct {
	Archiver  ARConfig
	BOSSWAVE  BWConfig
	Metadata  MDConfig
	BtrDB     BTRDBConfig
	Benchmark BenchmarkConfig
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
		log.Fatal("Could not find configuration files ./archiver.ini. Try running pundat makeconf")
	} else {
		return &configuration
	}
	return &configuration
}
