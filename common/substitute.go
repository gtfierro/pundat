package common

// implements a basic regex substitution interface for going between URIs and other paths. Just a simple
// API wrapper around capture groups, for the most part
// Example use cases:
//    1. reported: gvnMwdNvhD5ClAuF8SQzrp-Ywcjx9c1m4du9N5MRCXs=/sensors/s.hamilton/00126d070000005e/i.temperature/signal/operative
//	     want: ciee/hamilton/00126d070000005e/air_temp
//		 substitution: .+/s.hamilton/(.+)/i.temperature    =>   ciee/hamilton/$1/air_temp

type Substitution struct {
}

//func Substitute(pattern,
