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
