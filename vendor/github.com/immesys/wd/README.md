
# Global Watch Dog

The concept of global watchdogs are simple. It is a collection of timers, that must be reset periodically, and if they timeout, it is considered a fault. You can also fault them early or retire them. To keep things secure, yet simple, you require a token to interact with watchdogs with a given prefix.

# Downloading

To get the command line tool, go to [https://github.com/immesys/wd/releases](the releases page).

To get the go library, do

```bash
go get github.com/immesys/wd
```

To get the python library, do

```bash
pip install gwd
```

# Names

Watchdog names must consist of lowercase letters and numbers. You are allowed underscores (`_`) and periods (`.`). Note that in some visualization and alerting tools, periods will be treated as group delimiters, for example these watchdogs

```
dc.server1.disk_space
dc.server1.internet
dc.server2.disk_space
dc.server2.internet
dc.server2.memory
```

may be rendered as

```
dc:
 - server1:
   - disk_space
   - internet
 - server2:
   - disk_space
   - internet
   - memory
```

And alerts may delivered at the granularity of "dc.server2", not individual alerts for every watchdog.

# Authentication

To set up the authentication key, put your authentication token (in hex plaintext) in one of these places. These are in decreasing order of priority:

- `$WD_TOKEN` environment variable
- `.wd_token` file in your current directory
- `.wd_token` file in your `$HOME` directory
- `/etc/wd/token` file

To create a new token (e.g for a specific service or machine) simply run

```bash
#bash
wd auth the.new.prefix
```

```python
#python
import gwd
gwd.auth("the.new.prefix")
```

```go
//go
import "github.com/immesys/wd"
...
   wd.Auth("the.new.prefix")
```

# Kicking

To create a new watchdog, or reset an existing one, you need to *kick* it. You can also specify how long the timeout should be. If not specified, the default is 300 seconds, or 5 minutes.

```bash
#bash
wd kick my.new.watchdog
# or with timeout
wd kick my.new.watchdog 600
```

```python
#python
import gwd
gwd.kick("my.new.watchdog")
# or with timeout
gwd.kick("my.new.watchdog", 600)
```

```go
//go
import "github.com/immesys/wd"
...
   wd.Kick("my.new.watchdog", 600)
```

# Faulting

Sometimes you know something is wrong immediately, and instead of waiting for the timeout to occur, you want to immediately set the watchdog to a failed state. To do so, you *fault* the watchdog

```bash
#bash
wd fault my.new.watchdog "I know something is wrong"
```

```python
#python
import gwd
gwd.fault("my.new.watchdog", "I know something is wrong")
```

```go
//go
import "github.com/immesys/wd"
...
   wd.Fault("my.new.watchdog", "I know something is wrong")
```

# Retiring

To remove a watchdog, or group of watchdogs that are no longer useful, you can *retire* all watchdogs that begin with a prefix

```bash
#bash
wd retire my.new.
```

```python
#python
import gwd
gwd.retire("my.new.")
```

```go
//go
import "github.com/immesys/wd"
...
   wd.Retire("my.new.")
```

# Monitoring

You can send changes in watchdog status to a slack integration by using the *monitor* command. First get a slack integration URL which looks a bit like https://hooks.slack.com/services/AAAAAAAAA/BBBBBBBBBBBB/CCCCCCCCCCCCCCCCCCCC and then do

```bash
#bash
wd monitor slack my/prefix "https://hooks.slack.com/..."
```

```go
//go
import "github.com/immesys/wd"
...
   id, _ := wd.Monitor("my/prefix", wd.MonitorSlack, "https://hooks.slack.com/...")
```

This will print out the ID of your monitor. Save this ID because you will need it if you want to delete the monitor later:

```bash
#bash
wd delmonitor theid
```

```go
//go
import "github.com/immesys/wd"
...
   wd.DelMonitor("theid")
```

# Status

To see what's broken, you can get the *status* of all watchdogs with a given prefix

```bash
#bash
wd status my.
```

```python
#python
import gwd
stats = gwd.status("my.")
```

```go
//go
import "github.com/immesys/wd"
...
   stats,_ := wd.Status("my.")
```

To explain the data, lets look at the bash output:

```bash
  $ wd status m.
  STATE NAME     EXPIRE                           REASON
  KGOOD m.test.2 Thu, 01 Dec 2016 10:57:32 -0800  K
  FAULT m.test.3 Thu, 01 Dec 2016 10:52:46 -0800  FAULT:deliberate
  TMOUT m.test.5 Thu, 01 Dec 2016 10:33:24 -0800  K
```
There are three watchdogs here. The first is in the KGOOD state (kicked and good). The expiry is in the future.

The second has been deliberately faulted, as indicated by the FAULT state, and a reason starting with `FAULT:`. The final watchdog has timed out. It does not have a reason because there was no deliberate fault, but we can see that the expiry is in the past.

The Go and Python bindings have the same names for the fields, with the same meaning.

Note that this output is a little out of date, the current output has an additional field "CUMD" which is the cumulative downtime. 

# Tips and tricks

The command line tool has some additional features to make it easier to use in scripts. If you want to parse the output, it is useful to
print the status as tab delimited without the header:

```bash
$ wd status m. --tabsep --noheader
TMOUT	m.test.2	Thu, 01 Dec 2016 10:57:32 -0800	K
FAULT	m.test.3	Thu, 01 Dec 2016 10:52:46 -0800	FAULT:deliberate
TMOUT	m.test.5	Thu, 01 Dec 2016 10:33:24 -0800	K
```

This looks similar, but as it is tab separated we can use tools like `cut` very easily. Lets get a list of the times when watchdogs timed out:

```bash
$ wd status m. --tabsep --noheader | grep "^TMOUT" | cut -f 3
Thu, 01 Dec 2016 10:57:32 -0800
Thu, 01 Dec 2016 10:33:24 -0800
```

You can also add some color to the output with `--color` (failed watchdogs will be red), and you can reorder it so that failed watchdogs are listed
first (useful if you have lots of them) with `--badfirst`.

# REST interface

If you need support in languages other than bash, python and go, it is very easy to interact with the REST interface. There are three endpoints, and for availability you should attempt to communicate with all three before failing. The endpoints are geographically distributed, so should not all fail at the same time.

```
https://wd-a.steelcode.com
https://wd-b.steelcode.com
https://wd-c.steelcode.com
```

The URL patterns are:

```
GET /auth/{prefix}?hmac={hmac}
GET /kick/{name}?timeout={seconds}&hmac={hmac}
GET /fault/{name}?reason={reason}&hmac={hmac}
GET /status/{prefix}?hmac={hmac}&header={0/1}
GET /retire/{prefix}?hmac={hmac}
```

The only difficult part is calculating the hmac token, which is calculated as the sha-256 of the authentication token (in binary, 32 bytes) with the prefix or name  appended onto it.
