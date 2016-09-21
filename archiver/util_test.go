package archiver

import (
	"reflect"
	"testing"
)

func TestGetURIPrefixes(t *testing.T) {
	for _, test := range []struct {
		uri      string
		prefixes []string
	}{
		{
			"/a/b",
			[]string{"/a", "/a/b"},
		},
		{
			"/a",
			[]string{"/a"},
		},
		{
			"/a/b/!meta/tag",
			[]string{"/a", "/a/b"},
		},
	} {
		prefixes := GetURIPrefixes(test.uri)
		if reflect.DeepEqual(test.prefixes, prefixes) {
			t.Errorf("URI %s prefixes are %v but got %v", test.uri, test.prefixes, prefixes)
		}
	}
}

func TestMatchIgnoreLastN(t *testing.T) {
	for _, test := range []struct {
		uri     string
		prefix  string
		n       int
		matches bool
	}{
		{
			"/a",
			"/a",
			0,
			true,
		},
		{
			"/a/b/signal/d",
			"/a/b/signal/d/!meta/hello",
			2,
			true,
		},
		{
			"/a/b/signal/d",
			"/a/b/signal/d/!meta",
			2,
			false,
		},
		{
			"/a/b/signal/d",
			"/a/b/signal/d/!meta",
			1,
			true,
		},
		{
			"/services/s.top/pantry/i.top/signal/cpu",
			"/services/s.top/pantry/i.top/signal/cpu/!meta/UnitofMeasure",
			2,
			true,
		},
		{
			"/services/s.top/pantry/i.top/signal/cpu",
			"/services/s.top/pantry/i.top/signal/mem/!meta/UnitofMeasure",
			2,
			false,
		},
	} {
		matches := MatchIgnoreLastN(test.uri, test.prefix, test.n)
		if matches != test.matches {
			t.Errorf("For %s %s %d got %v but wanted %v", test.uri, test.prefix, test.n, matches, test.matches)
		}
	}
}
