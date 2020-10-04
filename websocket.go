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
	"errors"
	"fmt"
	"net/url"

	"github.com/gorilla/websocket"
)

var (
	defaultDialer = *websocket.DefaultDialer
)

func init() {
	defaultDialer.Proxy = nil // TODO use proxy env
	defaultDialer.EnableCompression = false
}

func (c *Conn) wsConnect() {
	uri := fmt.Sprintf("%s:%d", c.Conf.Host, c.Conf.Port)
	u := url.URL{
		Scheme: "ws",
		Host:   uri,
	}
	log.Debugf("EXA: connecting to %s", u.String())
	// According to documentation:
	// > It is safe to call Dialer's methods concurrently.
	ws, resp, err := defaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Debugf("resp:%s", resp)
		log.Fatal("dial:", err)
	}
	c.ws = ws
}

func (c *Conn) send(request interface{}) (map[string]interface{}, error) {
	receive, err := c.asyncSend(request)
	if err != nil {
		return nil, err
	}
	return receive()
}

func (c *Conn) asyncSend(request interface{}) (func() (map[string]interface{}, error), error) {
	err := c.ws.WriteJSON(request)
	if err != nil {
		c.error("Error sending:", err)
		return nil, err
	}

	return func() (map[string]interface{}, error) {
		var response map[string]interface{}
		var result map[string]interface{}
		err = c.ws.ReadJSON(&response)

		if err != nil {
			log.Panicf("Error recving: %s", err)
		} else if response["status"] != "ok" {
			exception := response["exception"].(map[string]interface{})
			err = errors.New(exception["text"].(string))
			c.error("Server Error:", err)
		} else if respData, ok := response["responseData"]; ok {
			result = respData.(map[string]interface{})
		} else if attr, ok := response["attributes"]; ok {
			// Some responses like getAttributes have no response data
			result = attr.(map[string]interface{})
		} else {
			// Some responses don't even have attr (like disconnect)
		}

		return result, err
	}, nil
}
