package querylang

import "testing"

func TestCleanTagString(t *testing.T) {
	var x, y, z string
	x = "/x/y/z,"
	y = cleantagstring(x)
	z = ".x.y.z"
	if y != z {
		t.Error(y, " should = ", z)
	}
}
