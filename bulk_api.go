/*
	Exasol supports a bulk IMPORT-EXPORT API that utilizes a
	proxy for sending data files (usually csv) to/from the server.

	This is the fastest way to import/export data.

	We support 2 interfaces, Bulk and Stream.

	In the Bulk interface you provide/receive the entire dataset
	in a single byte buffer. This can be more convenient but it
	can cause memory issues if your dataset is too large.

	In the Stream interface you provide/receive a chan of byte slices.
	When writing it's recommended that you break up your dataset into
	slices of about 10KB.
	When reading you will receive a series of slices in the 10KB range
	which you will need to concatenate to form the full dataset.


	For each of the Bulk & Streaming interfaces there are 4 possible interactions:

	1) "Insert" is for inserting into a single table with the data provided
	   mapping directly into the table columns

 	2) "Execute" allow you to do a bulk data import for arbitrarily complex
	   INSERT or MERGE statements. The DML provided must include an IMPORT
       statement similar to that in the getTableImportSQL routine below

	3) "Select" is for selecting out of a single table with the data received
	   mapping directly to the table's columns

	4) "Query" allows you to do a bulk data export from arbitrarily complex
   	   SELECT statements. The DQL provided must include an EXPORT
	   statement similar to that in the getTableExportSQL routine below


	TODO:
	1) Automate the sizing of incoming streamed slices


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
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"
)

func (c *Conn) BulkInsert(schema, table string, data *bytes.Buffer) (err error) {
	sql := c.getTableImportSQL(schema, table)
	return c.BulkExecute(sql, data)
}

func (c *Conn) BulkExecute(sql string, data *bytes.Buffer) error {
	if data == nil {
		return fmt.Errorf("You must pass in a bytes.Buffer pointer to BulkExecute")
	}
	dataChan := make(chan []byte, 1)
	dataChan <- data.Bytes()
	close(dataChan)
	return c.StreamExecute(sql, dataChan)
}

func (c *Conn) BulkSelect(schema, table string, data *bytes.Buffer) (err error) {
	sql := c.getTableExportSQL(schema, table)
	return c.BulkQuery(sql, data)
}

func (c *Conn) BulkQuery(sql string, data *bytes.Buffer) error {
	if data == nil {
		return fmt.Errorf("You must pass in a bytes.Buffer pointer to BulkQuery")
	}
	rows := c.StreamQuery(sql)
	for b := range rows.Data {
		data.Write(b)
	}
	if rows.Error != nil {
		return fmt.Errorf("Unable to BulkQuery: %s", rows.Error)
	}
	return nil
}

func (c *Conn) StreamInsert(schema, table string, data <-chan []byte) (err error) {
	sql := c.getTableImportSQL(schema, table)
	return c.StreamExecute(sql, data)
}

func (c *Conn) StreamExecute(origSQL string, data <-chan []byte) error {
	if data == nil {
		return fmt.Errorf("You must pass in a []byte chan to StreamExecute")
	}

	// Retry twice cuz it seems we sometimes get sentient errors
	for range []int{1, 2} {
		bytesWritten, err := c.streamExecuteNoRetry(origSQL, data)
		if err != nil {
			if retryableError(err) {
				if bytesWritten == 0 {
					c.error("Retrying...")
					continue
				}
				// If there was an error while writing the data
				// we've lost the data we've written so we can't retry
				c.error("Data already sent can't retry...")
			}
			c.error(err.Error())
			return err
		}
		break
	}
	return nil
}

func (c *Conn) StreamSelect(schema, table string) *Rows {
	sql := c.getTableExportSQL(schema, table)
	return c.StreamQuery(sql)
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 65524, 65524)
	},
}

func (c *Conn) StreamQuery(exportSQL string) *Rows {
	r := &Rows{
		Data: make(chan []byte, 1),
		Pool: &bufPool,
		conn: c,
		stop: make(chan bool, 1),
		wg:   sync.WaitGroup{},
	}

	// Asynchronously read in the data from Exasol
	r.wg.Add(1)
	go func() {
		defer func() {
			close(r.Data)
			r.wg.Done()
		}()

		// Retry once because for some reason we occasionally get "connection refused"
		// errors when Exasol tries to connect to the internal proxy that it set up.
		for i := 0; i <= 2; i++ {
			r.Error = r.streamQuery(exportSQL)
			if retryableError(r.Error) {
				c.error("Retrying...")
				r.Error = nil
				continue
			}
			return
		}
	}()

	return r
}

type Rows struct {
	BytesRead int64
	Data      chan []byte
	Pool      *sync.Pool // Use this to return the []bytes
	Error     error

	conn  *Conn
	proxy *Proxy
	stop  chan bool
	wg    sync.WaitGroup
}

func (r *Rows) Close() {
	origCfg := r.conn.Conf.SuppressError
	if r.proxy.IsRunning() {
		// Suppress errors from forcing it to stop
		r.conn.Conf.SuppressError = true
		select {
		case r.stop <- true:
		default:
		}
	}
	r.wg.Wait()
	r.conn.Conf.SuppressError = origCfg
}

/*--- Private Routines ---*/

func (r *Rows) streamQuery(exportSQL string) error {
	proxy, receiver, err := r.conn.initProxy(exportSQL)
	if err != nil {
		return err
	}
	r.proxy = proxy
	defer r.proxy.Shutdown()

	dataErr := make(chan error, 1)
	respErr := make(chan error, 1)
	go func() {
		// This is a blocking reader of the CSV data
		r.BytesRead, err = r.proxy.Read(r.Data, r.stop)
		dataErr <- err
	}()
	go func() {
		// This returns the result of the EXPORT query
		err := receiver(&response{})
		respErr <- err
	}()

	timeout := make(<-chan time.Time)
	if r.conn.Conf.QueryTimeout.Seconds() > 0 {
		timeout = time.After(r.conn.Conf.QueryTimeout)
	}

	select {
	case err = <-dataErr:
		if err == nil {
			err = <-respErr
		}
	case err = <-respErr:
		if err == nil {
			err = <-dataErr
		}
	case <-timeout:
		err = errors.New("Timed out doing BulkQuery")
	}

	// If we purposefully prematurely closed the connection
	// we don't want to raise any errors.
	if err != nil {
		r.conn.error("Unable to bulk export data: %s %s", exportSQL, err)
	}

	return err
}

func (c *Conn) streamExecuteNoRetry(origSQL string, data <-chan []byte) (
	bytesWritten int64, err error,
) {
	proxy, receiver, err := c.initProxy(origSQL)
	if err != nil {
		return 0, fmt.Errorf("Unable to import or export data: %s\n%s", origSQL, err)
	}
	defer proxy.Shutdown()

	dataErr := make(chan error, 1)
	respErr := make(chan error, 1)
	go func() {
		// This is a blocking writer of the CSV data
		var e error
		bytesWritten, e = proxy.Write(data)
		dataErr <- e
	}()
	go func() {
		// This returns the result of the IMPORT query
		e := receiver(&response{})
		respErr <- e
	}()

	timeout := make(<-chan time.Time)
	if c.Conf.QueryTimeout.Seconds() > 0 {
		timeout = time.After(c.Conf.QueryTimeout)
	}

	select {
	case err = <-dataErr:
		if err == nil {
			err = <-respErr
		}
	case err = <-respErr:
		if err == nil {
			err = <-dataErr
		}
	case <-timeout:
		err = fmt.Errorf("Timed out doing StreamExecute")
	}

	if err != nil {
		err = fmt.Errorf("Unable to import or export data: %s\n%s", origSQL, err)
	}

	return bytesWritten, err
}

func (c *Conn) initProxy(sql string) (*Proxy, func(interface{}) error, error) {
	proxy, err := NewProxy(c.Conf.Host, c.Conf.Port, &bufPool, c.log)
	if err != nil {
		c.error(err.Error())
		return nil, nil, err
	}

	proxyURL := fmt.Sprintf("http://%s:%d", proxy.Host, proxy.Port)
	sql = fmt.Sprintf(sql, proxyURL)

	req := &execReq{
		Command: "execute",
		SqlText: sql,
	}
	c.log.Debug("Stream sql: ", sql)
	receiver, err := c.asyncSend(req)
	if err != nil {
		c.error("Unable to stream sql: %s %s", sql, err)
		proxy.Shutdown()
		return nil, nil, err
	}

	return proxy, receiver, nil
}

func retryableError(err error) bool {
	retryableError := regexp.MustCompile(`failed after 0 bytes.+Connection refused`)
	if err != nil &&
		retryableError.MatchString(err.Error()) {
		return true
	}
	return false
}

func (c *Conn) getTableImportSQL(schema, table string) string {
	return fmt.Sprintf(
		"IMPORT INTO %s.%s FROM CSV AT '%%s' FILE 'data.csv'",
		c.QuoteIdent(schema), c.QuoteIdent(table),
	)
}

func (c *Conn) getTableExportSQL(schema, table string) string {
	return fmt.Sprintf(
		"EXPORT %s.%s INTO CSV AT '%%s' FILE 'data.csv'",
		c.QuoteIdent(schema), c.QuoteIdent(table),
	)
}
