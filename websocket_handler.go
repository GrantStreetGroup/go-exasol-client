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
	"crypto/tls"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
)

// This is the default websocket handler that uses gorilla/websocket implementation
// and conforms to the WSHandler interface

type defWSHandler struct {
	ws *websocket.Conn
}

func newDefaultWSHandler() *defWSHandler {
	return &defWSHandler{}
}

var defaultDialer = *websocket.DefaultDialer

func init() {
	defaultDialer.Proxy = nil // TODO use proxy env
	defaultDialer.EnableCompression = false
}

func (wsh *defWSHandler) Connect(url url.URL, tlsCfg *tls.Config, timeout time.Duration) error {
	if timeout != time.Duration(0) {
		defaultDialer.HandshakeTimeout = timeout
	}
	defaultDialer.TLSClientConfig = tlsCfg

	// According to documentation:
	// > It is safe to call Dialer's methods concurrently.
	ws, _, err := defaultDialer.Dial(url.String(), nil)
	if err != nil {
		return err
	}

	wsh.ws = ws
	return nil
}

func (wsh *defWSHandler) WriteJSON(req interface{}) error { return wsh.ws.WriteJSON(req) }
func (wsh *defWSHandler) ReadJSON(resp interface{}) error { return wsh.ws.ReadJSON(resp) }
func (wsh *defWSHandler) EnableCompression(e bool)        { wsh.ws.EnableWriteCompression(e) }
func (wsh *defWSHandler) Close() {
	wsh.ws.Close()
	wsh.ws = nil
}
