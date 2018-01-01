package version

import (
	"fmt"
)

var Commit = "unset"
var Release = "unset"

var LOGO = fmt.Sprintf("                      __     __ \n"+
	"   ___  __ _____  ___/ /__ _/ /_\n"+
	"  / _ \\/ // / _ \\/ _  / _ `/ __/\n"+
	" / .__/\\_,_/_//_/\\_,_/\\_,_/\\__/ \n"+
	"/_/                             \n"+
	"Commit: %s   Release: %s\n", Commit, Release)
