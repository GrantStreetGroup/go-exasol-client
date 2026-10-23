package exasol

import (
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProxyReadEOF(t *testing.T) {
	clientEnd, serverEnd := net.Pipe()
	defer clientEnd.Close()
	p := &Proxy{
		conn:    clientEnd,
		pool:    &bufPool,
		log:     customTestLogger("warning"),
		running: true,
		done:    make(chan struct{}),
	}

	go io.Copy(io.Discard, serverEnd) // Drain what the proxy writes back
	go func() {
		serverEnd.Write([]byte("PUT / HTTP/1.1\r\n\r\n"))
		serverEnd.Write([]byte("5\r\nhello\r\n"))
		serverEnd.Close()
	}()

	data := make(chan []byte, 10)
	stop := make(chan bool, 1)
	totalRead, err := p.Read(data, stop)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "Unable to read from proxy(2)")
	assert.Contains(t, err.Error(), "EOF")
	assert.NotContains(t, err.Error(), "parse chunkSize")
	assert.Equal(t, int64(5), totalRead)
	require.Len(t, data, 1)
	assert.Equal(t, "hello", string(<-data))
}
