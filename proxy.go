/*
	This sets up the proxy server connection that Exasol
	uses for doing bulk IMPORTs and EXPORTs

    AUTHOR

	Grant Street Group <developers@grantstreet.com>

	COPYRIGHT AND LICENSE

	This software is Copyright (c) 2019 by Grant Street Group.
	This is free software, licensed under:
	    MIT License
*/

package exasol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/op/go-logging"
)

type Proxy struct {
	Host string
	Port uint32

	conn    net.Conn
	running bool
	pool    *sync.Pool
	log     *logging.Logger
}

func NewProxy(host string, port uint16, bufPool *sync.Pool, log *logging.Logger) (*Proxy, error) {
	p := &Proxy{
		pool: bufPool,
		log:  log,
	}

	var err error
	uri := fmt.Sprintf("%s:%d", host, port)
	p.conn, err = net.Dial("tcp", uri)
	if err != nil {
		return nil, fmt.Errorf("Unable to setup proxy (1): %s", err)
	}
	p.running = true

	// This asks Exasol to setup a proxy connected to this socket
	req := make([]byte, 12)
	binary.LittleEndian.PutUint32(req[0:], 0x02212102)
	binary.LittleEndian.PutUint32(req[4:], 1)
	binary.LittleEndian.PutUint32(req[8:], 1)
	_, err = p.conn.Write(req)
	if err != nil {
		return nil, fmt.Errorf("Unable to setup proxy (2): %s", err)
	}

	// Exasol replies with the internal host/port it's listening on
	resp := make([]byte, 24)
	_, err = p.conn.Read(resp)
	if err != nil {
		return nil, fmt.Errorf("Unable to setup proxy (3): %s", err)
	}

	p.Port = binary.LittleEndian.Uint32(resp[4:])
	p.Host = string(bytes.Trim(resp[8:], "\x00")) // Remove nulls
	p.log.Debugf("Proxy is %s:%d", p.Host, p.Port)

	return p, nil
}

func (p *Proxy) Read(data chan<- []byte, stop <-chan bool) (int64, error) {
	_, err := p.readHeaders()
	if err != nil {
		return 0, err
	}

	p.sendHeaders([]string{
		"HTTP/1.1 100 Continue",
		"Content-Length: 0",
		"Connection: close",
	})

	// Read chunks
	var totalRead int64
DATA:
	for {
		chunkSize, err := p.readLine()
		if err != nil {
			return totalRead, fmt.Errorf("Unable to read from proxy(2): %s", err)
		}

		chunkLen, err := strconv.ParseInt(string(chunkSize), 16, 64)
		if err != nil {
			return totalRead, fmt.Errorf("Unable to parse chunkSize %s: %s", chunkSize, err)
		}
		chunk := p.pool.Get().([]byte)
		if chunkLen > int64(cap(chunk)) {
			p.log.Warningf("Proxy chunk len %d > buffer cap %d", chunkLen, cap(chunk))
			chunk = make([]byte, chunkLen)
		} else if chunkLen != int64(len(chunk)) {
			chunk = chunk[:chunkLen]
		}

		readLen := 0
		for {
			l, err := p.conn.Read(chunk[readLen:])
			if err != nil {
				return totalRead, fmt.Errorf("Unable to read from proxy(3): %s", err)
			}
			readLen += l
			if int64(readLen) == chunkLen {
				break
			}
		}
		endOfChunk, err := p.readLine()
		if len(endOfChunk) != 0 || err != nil {
			return totalRead, fmt.Errorf("Unable to read from proxy(4):%s/%s", endOfChunk, err)
		}

		if chunkLen == 0 {
			// Last chunk so wrap up and head out
			p.sendHeaders([]string{
				"HTTP/1.1 200 OK",
				"Content-Length: 0",
				"Connection: close",
			})
			break
		}

		totalRead += chunkLen
		select {
		case <-stop:
			p.Shutdown()
			break DATA
		case data <- chunk:
		}
	}

	return totalRead, nil
}

func (p *Proxy) Write(data <-chan []byte) (bytesWritten int64, err error) {
	_, err = p.readHeaders()
	if err != nil {
		return bytesWritten, err
	}

	err = p.sendHeaders([]string{
		"HTTP/1.1 200 OK",
		"Content-Type: application/octet-stream",
		"Content-Disposition: attachment; filename=data.csv",
		"Transfer-Encoding: chunked",
		"Connection: close",
	})

	if err != nil {
		err = fmt.Errorf("Unable to send headers to proxy: %s", err)
	} else {
		for b := range data {
			l := int64(len(b))
			bytesWritten += l
			chunkSize := strconv.FormatInt(l, 16)
			p.conn.Write([]byte(chunkSize))
			p.conn.Write([]byte("\r\n"))
			_, err = p.conn.Write(b)
			if err != nil {
				err = fmt.Errorf("Unable to upload data to proxy (2): %s", err)
				break
			}
			p.conn.Write([]byte("\r\n"))
		}
		p.conn.Write([]byte("0\r\n\r\n")) // A final zero chunk
	}
	return bytesWritten, err
}

func (p *Proxy) Shutdown() {
	if p.IsRunning() {
		if p.conn != nil {
			p.conn.Close()
		}
		p.running = false
	}
}

func (p *Proxy) IsRunning() bool {
	return p.running
}

/* Private routines */

func (p *Proxy) readLine() ([]byte, error) {
	var line bytes.Buffer
	var err error
	b := make([]byte, 1)
	for {
		length, err := p.conn.Read(b)
		if err != nil || length == 0 {
			break
		} else if b[0] == '\r' {
			// Look ahead at next byte
			length, err = p.conn.Read(b)
			if err != nil || length == 0 || b[0] == '\n' {
				// End of line
				break
			}
			line.WriteByte('\r')
		}
		line.Write(b)
	}
	return line.Bytes(), err
}

func (p *Proxy) sendHeaders(headers []string) error {
	headers = append(headers, "")
	for _, header := range headers {
		header += "\r\n"
		p.log.Debug("Sent Header: ", header)
		_, err := p.conn.Write([]byte(header))
		if err != nil {
			return fmt.Errorf("Unable to send header <%s>to proxy: %s", header, err)
		}
	}
	return nil
}

func (p *Proxy) readHeaders() (headers []string, err error) {
	for {
		line, err := p.readLine()
		if err != nil {
			return headers, fmt.Errorf("Unable to read from proxy(1): %s", err)
		}
		p.log.Debug("Got header:", string(line))
		// Blank line means end of headers
		if len(line) == 0 {
			break
		}
		headers = append(headers, string(line))
	}
	return headers, nil
}
