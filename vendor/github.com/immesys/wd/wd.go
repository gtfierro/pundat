package wd

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var _endpoints = []string{"https://wd-a.steelcode.com",
	"https://wd-b.steelcode.com",
	"https://wd-c.steelcode.com"}
var authtoken string
var pat = regexp.MustCompile("^[a-z0-9\\._]+$")

const MonitorSlack = 1
const ReqTimeout = 5 * time.Second

type atype struct {
	isKick bool
	when   time.Time
}

func (a *atype) OlderThan(d time.Duration) bool {
	return time.Now().Sub(a.when) > d
}

var rlmap map[string]atype
var rlLock sync.Mutex

func getEndpoints() []string {
	rand.Seed(time.Now().UnixNano())
	idz := rand.Perm(len(_endpoints))
	rv := make([]string, len(_endpoints))
	for frm, to := range idz {
		rv[frm] = _endpoints[to]
	}
	return rv
}
func init() {
	rlmap = make(map[string]atype)
	authtoken = os.Getenv("WD_TOKEN")
	if authtoken != "" {
		authtoken = authtoken[:64]
		return
	}
	blob, err := ioutil.ReadFile(".wd_token")
	if err == nil {
		authtoken = string(blob[:64])
		return
	}
	blob, err = ioutil.ReadFile(os.Getenv("HOME") + "/.wd_token")
	if err == nil {
		authtoken = string(blob[:64])
		return
	}
	blob, err = ioutil.ReadFile("/etc/wd/token")
	if err == nil {
		authtoken = string(blob[:64])
		return
	}
	fmt.Fprintf(os.Stderr, "Could not find a watchdog authentication token\n")
	os.Exit(1)
}
func ValidPrefix(prefix string) bool {
	return pat.MatchString(prefix)
}
func Kick(name string, seconds int) error {
	if !ValidPrefix(name) {
		panic("Watchdog kick with invalid name: " + name)
	}
	token, err := hex.DecodeString(authtoken)
	if err != nil {
		panic("Watchdog invalid token: " + err.Error())
	}
	body := make([]byte, 32+len(name))
	copy(body, token)
	copy(body[32:], []byte(name))
	hmac := sha256.Sum256(body)
	for _, endpoint := range getEndpoints() {
		timeout := time.Duration(ReqTimeout)
		client := http.Client{
			Timeout: timeout,
		}
		resp, err := client.Get(fmt.Sprintf("%s/kick/%s?timeout=%d&hmac=%064x", endpoint, name, seconds, hmac))
		if err != nil {
			fmt.Fprintf(os.Stderr, "WD endpoint %s error: %s\n", endpoint, err.Error())
			continue
		} else {
			contents, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			} else {
				return errors.New(string(contents))
			}
		}
	}
	return fmt.Errorf("No endpoints reachable")
}
func Fault(name string, reason string) error {
	if !ValidPrefix(name) {
		panic("Watchdog fault with invalid name: " + name)
	}
	token, err := hex.DecodeString(authtoken)
	if err != nil {
		panic("Watchdog invalid token: " + err.Error())
	}
	body := make([]byte, 32+len(name))
	copy(body, token)
	copy(body[32:], []byte(name))
	hmac := sha256.Sum256(body)
	for _, endpoint := range getEndpoints() {
		timeout := time.Duration(ReqTimeout)
		client := http.Client{
			Timeout: timeout,
		}
		resp, err := client.Get(fmt.Sprintf("%s/fault/%s?reason=%s&hmac=%064x", endpoint, name, url.QueryEscape(reason), hmac))
		if err != nil {
			fmt.Fprintf(os.Stderr, "WD endpoint %s error: %s\n", endpoint, err.Error())
			continue
		} else {
			contents, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			} else {
				return errors.New(string(contents))
			}
		}
	}
	return fmt.Errorf("No endpoints reachable")
}

// RLFault is like Fault but it will only send 1 fault per interval. Returns
// true if an actual fault was done, false otherwise
func RLFault(interval time.Duration, name string, reason string) bool {
	rlLock.Lock()
	prev, ok := rlmap[name]
	if ok && !prev.isKick && !prev.OlderThan(interval) {
		rlLock.Unlock()
		return false
	}
	rlmap[name] = atype{isKick: false, when: time.Now()}
	rlLock.Unlock()
	Fault(name, reason)
	return true
}

// RLKick is like Kick but it will only send 1 kick per interval. Returns
// true if an actual kick was done, false otherwise
func RLKick(interval time.Duration, name string, timeout int) bool {
	rlLock.Lock()
	prev, ok := rlmap[name]
	if ok && prev.isKick && !prev.OlderThan(interval) {
		rlLock.Unlock()
		return false
	}
	rlmap[name] = atype{isKick: true, when: time.Now()}
	rlLock.Unlock()
	Kick(name, timeout)
	return true
}
func Auth(prefix string) (string, error) {
	if !ValidPrefix(prefix) {
		panic("Watchdog auth with invalid prefix: " + prefix)
	}
	token, err := hex.DecodeString(authtoken)
	if err != nil {
		panic("Watchdog invalid token: " + err.Error())
	}
	body := make([]byte, 32+len(prefix))
	copy(body, token)
	copy(body[32:], []byte(prefix))
	hmac := sha256.Sum256(body)
	for _, endpoint := range getEndpoints() {
		timeout := time.Duration(ReqTimeout)
		client := http.Client{
			Timeout: timeout,
		}
		resp, err := client.Get(fmt.Sprintf("%s/auth/%s?hmac=%064x", endpoint, prefix, hmac))
		if err != nil {
			fmt.Fprintf(os.Stderr, "WD endpoint %s error: %s\n", endpoint, err.Error())
			continue
		} else {
			contents, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return string(contents)[:64], nil
			} else {
				return "", fmt.Errorf("%s", contents)
			}
		}
	}
	return "", fmt.Errorf("No endpoints reachable")
}

type WDStatus struct {
	Status   string
	Expires  string
	Name     string
	Reason   string
	CumDTime time.Duration
}

func Status(prefix string) ([]WDStatus, error) {
	if !ValidPrefix(prefix) {
		panic("Watchdog status with invalid prefix: " + prefix)
	}
	token, err := hex.DecodeString(authtoken)
	if err != nil {
		panic("Watchdog invalid token: " + err.Error())
	}
	body := make([]byte, 32+len(prefix))
	copy(body, token)
	copy(body[32:], []byte(prefix))
	hmac := sha256.Sum256(body)
	for _, endpoint := range getEndpoints() {
		if os.Getenv("WD_DEBUG_ENDPOINT") != "" {
			fmt.Printf("Trying endpoint %s\n", endpoint)
		}
		timeout := time.Duration(ReqTimeout)
		client := http.Client{
			Timeout: timeout,
		}
		resp, err := client.Get(fmt.Sprintf("%s/status/%s?hmac=%064x&header=0", endpoint, prefix, hmac))
		if err != nil {
			fmt.Fprintf(os.Stderr, "WD endpoint %s error: %s\n", endpoint, err.Error())
			continue
		} else {
			rv := make([]WDStatus, 0, 30)
			if resp.StatusCode != 200 {
				contents, _ := ioutil.ReadAll(resp.Body)
				resp.Body.Close()
				return nil, errors.New(string(contents))
			}
			reader := bufio.NewReader(resp.Body)
			l, err := reader.ReadString('\n')
			for err == nil {
				parts := strings.Split(l, "\t")
				cumd, _ := strconv.ParseInt(parts[3], 10, 64)
				st := WDStatus{Status: parts[0], Expires: parts[1], Name: parts[2], Reason: parts[4], CumDTime: time.Duration(cumd) * time.Millisecond}
				rv = append(rv, st)
				l, err = reader.ReadString('\n')
			}
			resp.Body.Close()
			return rv, nil
		}
	}
	return nil, errors.New("no endpoints reachable")
}
func Retire(prefix string) error {
	if !ValidPrefix(prefix) {
		panic("Watchdog retire with invalid prefix: " + prefix)
	}
	token, err := hex.DecodeString(authtoken)
	if err != nil {
		panic("Watchdog invalid token: " + err.Error())
	}
	body := make([]byte, 32+len(prefix))
	copy(body, token)
	copy(body[32:], []byte(prefix))
	hmac := sha256.Sum256(body)
	for _, endpoint := range getEndpoints() {
		timeout := time.Duration(ReqTimeout)
		client := http.Client{
			Timeout: timeout,
		}
		resp, err := client.Get(fmt.Sprintf("%s/retire/%s?hmac=%064x", endpoint, prefix, hmac))
		if err != nil {
			fmt.Fprintf(os.Stderr, "WD endpoint %s error: %s\n", endpoint, err.Error())
			continue
		} else {
			contents, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			} else {
				return errors.New(string(contents))
			}
		}
	}
	return errors.New("no endpoints reachable")
}
func Clear(prefix string) error {
	if !ValidPrefix(prefix) {
		panic("Watchdog clear with invalid prefix: " + prefix)
	}
	token, err := hex.DecodeString(authtoken)
	if err != nil {
		panic("Watchdog invalid token: " + err.Error())
	}
	body := make([]byte, 32+len(prefix))
	copy(body, token)
	copy(body[32:], []byte(prefix))
	hmac := sha256.Sum256(body)
	for _, endpoint := range getEndpoints() {
		timeout := time.Duration(ReqTimeout)
		client := http.Client{
			Timeout: timeout,
		}
		resp, err := client.Get(fmt.Sprintf("%s/clear/%s?hmac=%064x", endpoint, prefix, hmac))
		if err != nil {
			fmt.Fprintf(os.Stderr, "WD endpoint %s error: %s\n", endpoint, err.Error())
			continue
		} else {
			contents, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			} else {
				return errors.New(string(contents))
			}
		}
	}
	return errors.New("no endpoints reachable")
}
func Monitor(prefix string, monitorType int, args string) (string, error) {
	if !ValidPrefix(prefix) {
		panic("Watchdog monitor with invalid prefix: " + prefix)
	}
	token, err := hex.DecodeString(authtoken)
	if err != nil {
		panic("Watchdog invalid token: " + err.Error())
	}
	body := make([]byte, 32+len(prefix))
	copy(body, token)
	copy(body[32:], []byte(prefix))
	hmac := sha256.Sum256(body)
	for _, endpoint := range getEndpoints() {
		timeout := time.Duration(ReqTimeout)
		client := http.Client{
			Timeout: timeout,
		}
		resp, err := client.Get(fmt.Sprintf("%s/monitor/%d/%s?hmac=%064x&args=%s", endpoint, monitorType, prefix, hmac, url.QueryEscape(args)))
		if err != nil {
			fmt.Fprintf(os.Stderr, "WD endpoint %s error: %s\n", endpoint, err.Error())
			continue
		}
		contents, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 200 {
			return string(contents), nil
		}
		return "", errors.New(string(contents))
	}
	return "", errors.New("no endpoints reachable")
}

func DeleteMonitor(id string) error {
	for _, endpoint := range getEndpoints() {
		timeout := time.Duration(ReqTimeout)
		client := http.Client{
			Timeout: timeout,
		}
		resp, err := client.Get(fmt.Sprintf("%s/delmonitor/%s", endpoint, id))
		if err != nil {
			fmt.Fprintf(os.Stderr, "WD endpoint %s error: %s\n", endpoint, err.Error())
			continue
		}
		contents, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 200 {
			return nil
		}
		return errors.New(string(contents))
	}
	return errors.New("no endpoints reachable")
}
