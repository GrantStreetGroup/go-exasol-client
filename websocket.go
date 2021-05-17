/*
    AUTHOR

	Grant Street Group <developers@grantstreet.com>

	COPYRIGHT AND LICENSE

	This software is Copyright (c) 2019 by Grant Street Group.
	This is free software, licensed under:
	    MIT License
*/

package exasol

import (
	"fmt"
	"math/rand"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

var (
	defaultDialer = *websocket.DefaultDialer
)

func init() {
	defaultDialer.Proxy = nil // TODO use proxy env
	defaultDialer.EnableCompression = false
}

func (c *Conn) wsConnect() (err error) {
	host := c.Conf.Host

	isIPRange := regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)\.(\d+)\.\.(\d+)$`)
	if isIPRange.MatchString(host) {
		// This is an IP range so choose a node at random to connect to.
		// If that connection fails try another one.
		ipRange := isIPRange.FindStringSubmatch(host)
		fromN, _ := strconv.ParseInt(ipRange[4], 10, 32)
		toN, _ := strconv.ParseInt(ipRange[5], 10, 32)
		ips := []string{}
		for i := fromN; i <= toN; i++ {
			ips = append(ips, fmt.Sprintf("%s.%s.%s.%d", ipRange[1], ipRange[2], ipRange[3], i))
		}
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(ips), func(i, j int) { ips[i], ips[j] = ips[j], ips[i] })

		for _, ip := range ips {
			err = c.wsConnectHost(ip)
			if err == nil {
				break
			}
		}
	} else {
		err = c.wsConnectHost(host)
	}

	return err
}

func (c *Conn) wsConnectHost(host string) error {
	uri := fmt.Sprintf("%s:%d", host, c.Conf.Port)
	scheme := "ws"
	if c.Conf.TLSConfig != nil {
		scheme = "wss"
	}
	u := url.URL{
		Scheme: scheme,
		Host:   uri,
	}
	c.log.Debugf("Connecting to %s", u.String())

	if c.Conf.ConnectTimeout != time.Duration(0) {
		defaultDialer.HandshakeTimeout = c.Conf.ConnectTimeout
	}
	defaultDialer.TLSClientConfig = c.Conf.TLSConfig

	// According to documentation:
	// > It is safe to call Dialer's methods concurrently.
	ws, resp, err := defaultDialer.Dial(u.String(), nil)
	if err != nil {
		c.log.Debugf("resp:%s", resp)
		return err
	}
	c.ws = ws
	return nil
}

// Request and Response are pointers to structs representing the API JSON.
// The Response struct is updated in-place.

func (c *Conn) send(request, response interface{}) error {
	receiver, err := c.asyncSend(request)
	if err != nil {
		return err
	}
	return receiver(response)
}

func (c *Conn) asyncSend(request interface{}) (func(interface{}) error, error) {
	err := c.ws.WriteJSON(request)
	if err != nil {
		return nil, c.error("WebSocket API Error sending: %s", err)
	}

	return func(response interface{}) error {
		err = c.ws.ReadJSON(response)
		if err != nil {
			if regexp.MustCompile(`abnormal closure`).
				MatchString(err.Error()) {
				return fmt.Errorf("Server terminated statement")
			}
			return fmt.Errorf("WebSocket API Error recving: %s", err)
		}
		r := reflect.Indirect(reflect.ValueOf(response))
		status := r.FieldByName("Status").String()
		if status != "ok" {
			err := reflect.Indirect(r.FieldByName("Exception")).
				FieldByName("Text").String()
			return fmt.Errorf("Server Error: %s", err)
		}
		return nil
	}, nil
}
