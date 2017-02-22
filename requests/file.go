package requests

import (
	"fmt"
	bw2 "github.com/immesys/bw2bind"
	"github.com/pkg/errors"
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
	"strings"
)

// Struct representation of a configuration file for attaching archive
// metadata. Follows the basic structure:
//
//    Prefix: gabe.pantry/services
//    Archive:
//      - URI: s.TED/MTU1/i.meter/signal/Voltage
//        Value: Value
//      - URI: s.TED/MTU1/i.meter/signal/PowerNow
//        Value: Value
//      - URI: s.TED/MTU1/i.meter/signal/KVA
//        Value: Value
type Config struct {
	Prefix               string                `yaml:"Prefix"`
	DummyArchiveRequests []DummyArchiveRequest `yaml:"Archive"`
}

type DummyArchiveRequest struct {
	AttachURI       string `yaml:"AttachURI"`
	ArchiveURI      string `yaml:"ArchiveURI"`
	PO              string `yaml:"PO"`
	UUIDExpr        string `yaml:"UUID"`
	ValueExpr       string `yaml:"Value"`
	TimeExpr        string `yaml:"Time"`
	TimeParse       string `yaml:"TimeParse"`
	Name            string `yaml:"Name"`
	InheritMetadata string `yaml:"InheritMetadata",omitempty`
}

func (d DummyArchiveRequest) ToArchiveRequest() *ArchiveRequest {
	var doinherit = true
	if d.InheritMetadata == "false" {
		doinherit = false
	}
	req := &ArchiveRequest{
		URI:             d.ArchiveURI,
		AttachURI:       d.AttachURI,
		PO:              bw2.FromDotForm(d.PO),
		UUIDExpr:        d.UUIDExpr,
		ValueExpr:       d.ValueExpr,
		TimeExpr:        d.TimeExpr,
		TimeParse:       d.TimeParse,
		Name:            d.Name,
		InheritMetadata: doinherit,
	}

	if d.AttachURI == "" {
		d.AttachURI = d.ArchiveURI
	}

	return req
}

func ReadConfig(filename string) (*Config, error) {
	fmt.Printf("Reading from config file %s\n", filename)
	var config = new(Config)
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return config, errors.Wrap(err, fmt.Sprintf("Could not read config file %v", filename))
	}
	if err := yaml.Unmarshal(bytes, config); err != nil {
		return config, errors.Wrap(err, "Could not unmarshal config file")
	}
	config.Prefix = strings.TrimSuffix(config.Prefix, "/")

	if len(config.DummyArchiveRequests) == 0 {
		return config, errors.New("Need to provide archive requests")
	}
	for i, req := range config.DummyArchiveRequests {
		req.ArchiveURI = config.Prefix + "/" + strings.TrimPrefix(req.ArchiveURI, "/")
		req.AttachURI = config.Prefix + "/" + strings.TrimPrefix(req.AttachURI, "/")
		if req.PO == "" {
			req.PO = "2.0.0.0"
		}
		config.DummyArchiveRequests[i] = req
	}

	return config, nil
}
