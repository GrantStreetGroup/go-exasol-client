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
	"fmt"
	"time"
)

func (c *Conn) BulkInsert(schema, table string, data *bytes.Buffer) (err error) {
	sql := getTableImportSQL(schema, table)
	return c.BulkExecute(sql, data)
}

func (c *Conn) BulkExecute(sql string, data *bytes.Buffer) error {
	if data == nil {
		c.log.Fatal("You must pass in a bytes.Buffer pointer to BulkExecute")
	}
	dataChan := make(chan []byte, 1)
	dataChan <- data.Bytes()
	close(dataChan)
	return c.StreamExecute(sql, dataChan)
}

func (c *Conn) BulkSelect(schema, table string, data *bytes.Buffer) (err error) {
	sql := getTableExportSQL(schema, table)
	return c.BulkQuery(sql, data)
}

func (c *Conn) BulkQuery(sql string, data *bytes.Buffer) error {
	if data == nil {
		c.log.Fatal("You must pass in a bytes.Buffer pointer to BulkQuery")
	}
	dataChan := make(chan []byte, 1)
	go func() {
		for b := range dataChan {
			data.Write(b)
		}
	}()
	_, err := c.StreamQuery(sql, dataChan)
	return err
}

func (c *Conn) StreamInsert(schema, table string, data <-chan []byte) (err error) {
	sql := getTableImportSQL(schema, table)
	return c.StreamExecute(sql, data)
}

func (c *Conn) StreamExecute(origSQL string, data <-chan []byte) error {
	if data == nil {
		c.log.Fatal("You must pass in a []byte chan to StreamExecute")
	}

	sentData := false
	var lastErr error
	// Retry twice cuz it seems we sometimes get sentient errors
	for i := range []int{1, 2} {
		if i > 0 {
			// If there was an error while writing the data
			// we've lost the data we've written so we can't retry
			if sentData {
				c.error("Data already sent can't retry...")
				break
			}
			c.error("Retrying...")
		}

		proxy, err := NewProxy(c.Conf.Host, c.Conf.Port)
		if err != nil {
			c.error(err)
			lastErr = err
			continue
		}
		defer proxy.Shutdown()

		proxyURL := fmt.Sprintf("http://%s:%d", proxy.Host, proxy.Port)
		sql := fmt.Sprintf(origSQL, proxyURL)

		req := &executeStmtJSON{
			Command: "execute",
			SQLtext: sql,
		}
		c.log.Info("EXA: Execute Import: ", sql)
		response, err := c.asyncSend(req)
		if err != nil {
			c.error("Unable to import bulk import data:", sql, err)
			lastErr = err
			continue
		}

		// This needs some sort of timeout because if the
		// SQL has an error the write call will just hang
		// because nothing is listening to it on the other end.
		// In that case the response() call below will return an error.
		// See the corresponding timeout in StreamQuery
		var uploadErr error
		sentData, uploadErr = proxy.Write(data)

		_, err = response()
		if err != nil || uploadErr != nil {
			c.error("Unable to import bulk import data:", sql, err, uploadErr)
			lastErr = err
			if err == nil {
				lastErr = uploadErr
			}
			continue // Retry
		} else {
			lastErr = nil
			break
		}
	}
	return lastErr
}

func (c *Conn) StreamSelect(schema, table string, data chan<- []byte) (int64, error) {
	sql := getTableExportSQL(schema, table)
	return c.StreamQuery(sql, data)
}

func (c *Conn) StreamQuery(origSQL string, data chan<- []byte) (int64, error) {
	if data == nil {
		c.log.Fatal("You must pass in a []byte chan to StreamQuery")
	}

	var bytesRead int64
	var lastErr error
	// Retry twice cuz it seems we sometimes get sentient errors
	for i := range []int{1, 2} {
		if i > 0 {
			c.error("Retrying...")
		}

		proxy, err := NewProxy(c.Conf.Host, c.Conf.Port)
		if err != nil {
			c.error(err)
			lastErr = err
			continue
		}
		defer proxy.Shutdown()

		proxyURL := fmt.Sprintf("http://%s:%d", proxy.Host, proxy.Port)
		sql := fmt.Sprintf(origSQL, proxyURL)

		req := &executeStmtJSON{
			Command: "execute",
			SQLtext: sql,
		}
		c.log.Info("EXA: Execute Execute: ", sql)
		response, err := c.asyncSend(req)
		if err != nil {
			c.error("Unable to bulk export data:", sql, err)
			lastErr = err
			continue
		}

		d := make(chan error, 1)
		r := make(chan error, 1)
		go func() {
			bytesRead, err = proxy.Read(data)
			d <- err
		}()
		go func() {
			_, err := response()
			r <- err
		}()

		select {
		case err = <-d:
			if err == nil {
				err = <-r
			}
		case err = <-r:
			if err == nil {
				err = <-d
			}
		case <-time.After(time.Duration(c.Conf.Timeout) * time.Second):
			err = fmt.Errorf("Timed out doing BulkQuery")
		}

		if err != nil {
			c.error("Unable to bulk export data:", sql, err)
			lastErr = err
			continue // Retry
		} else {
			lastErr = nil
			break
		}
	}
	return bytesRead, lastErr
}

/*--- Private Routines ---*/

func getTableImportSQL(schema, table string) string {
	return fmt.Sprintf(
		"IMPORT INTO %s.%s FROM CSV AT '%%s' FILE 'data.csv'",
		schema, table,
	)
}

func getTableExportSQL(schema, table string) string {
	return fmt.Sprintf(
		"EXPORT %s.%s INTO CSV AT '%%s' FILE 'data.csv'",
		schema, table,
	)
}
