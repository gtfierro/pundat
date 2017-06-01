package scraper

import (
	ldbutil "github.com/syndtr/goleveldb/leveldb/util"
	"strings"
)

var metasuffix = []byte("/!meta")
var metasuffixstr = "/!meta"

func getURIKey(uri string) string {
	li := strings.LastIndex(uri, "/")
	if li > 0 {
		return uri[li+1:]
	}
	return uri
}

// returns the uri stripped of "/!meta/<keyname>"
func getStrippedURI(uri string) string {
	li := strings.LastIndex(uri, metasuffixstr)
	if li > 0 {
		return uri[:li]
	}
	return uri
}

func BytesPrefix(prefix []byte) *ldbutil.Range {
	var newpfx = make([]byte, len(prefix))
	copy(newpfx, prefix)
	newpfx = append(newpfx, metasuffix...)
	var limit []byte
	for i := len(newpfx) - 1; i >= 0; i-- {
		c := newpfx[i]
		if c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, newpfx)
			limit[i] = c + 1
			break
		}
	}
	return &ldbutil.Range{newpfx, limit}
}
