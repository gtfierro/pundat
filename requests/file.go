package requests

import (
	"fmt"
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
	AttachURI  string `yaml:"AttachURI"`
	ArchiveURI string `yaml:"ArchiveURI"`
	PO         string `yaml:"PO"`
	UUIDExpr   string `yaml:"UUID"`
	ValueExpr  string `yaml:"Value"`
	TimeExpr   string `yaml:"Time"`
	TimeParse  string `yaml:"TimeParse"`
	URIMatch   string `yaml:"URIMatch"`
	URIReplace string `yaml:"URIReplace"`
	Name       string `yaml:"Name"`
	Unit       string `yaml:"Unit"`
}

func (d DummyArchiveRequest) ToArchiveRequest() *ArchiveRequest {
	req := &ArchiveRequest{
		URI:        d.ArchiveURI,
		AttachURI:  d.AttachURI,
		PO:         d.PO,
		UUIDExpr:   d.UUIDExpr,
		ValueExpr:  d.ValueExpr,
		TimeExpr:   d.TimeExpr,
		TimeParse:  d.TimeParse,
		URIMatch:   d.URIMatch,
		URIReplace: d.URIReplace,
		Name:       d.Name,
		Unit:       d.Unit,
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
