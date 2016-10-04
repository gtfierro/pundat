package dots

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Given a URI of form archive/start/<t1>/end/<t2>/<uri>,
// returns t1, t2, uri. Errors if the URI does not start with the archive
// namespace (either "archive" or LMBbz7H1DeJICLTrEZk5EEpFGGmrZu-KiCllttWyZsI=)
func parseArchiveURI(archiveuri string) (start, end time.Time, uri string, err error) {
	var (
		start_nano, end_nano int64
	)
	parts := strings.Split(archiveuri, "/")
	if parts[0] != "archive" && parts[0] != ArchiveVK {
		err = fmt.Errorf("URI %s did not come from archive namespace", archiveuri)
		return
	}
	if parts[2] == "+" {
		start = time.Unix(0, Beginning)
	} else {
		start_nano, err = strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return
		}
		start = time.Unix(0, start_nano)
	}

	if parts[4] == "+" {
		end = time.Unix(0, EndOfTime)
	} else {
		end_nano, err = strconv.ParseInt(parts[4], 10, 64)
		if err != nil {
			return
		}
		end = time.Unix(0, end_nano)
	}

	uri = strings.Join(parts[5:], "/")
	return
}

// returns the <uri> component of "archive/start/+/end+/<uri>"
func getURIFromArchiveURI(archiveuri string) string {
	return strings.Join(strings.Split(archiveuri, "/")[5:], "/")
}

func getURIFromSuffix(urisuffix string) string {
	return strings.Join(strings.Split(urisuffix, "/")[4:], "/")
}
