package exasol

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// Test various connection options:

func (s *testSuite) TestConnClientName() {
	conf := s.connConf()
	conf.ClientName = "MyTester"
	conf.ClientVersion = "123"
	c, err := Connect(conf)
	s.Nil(err, "No connection errors")

	got, _ := c.FetchSlice(`
		SELECT client
		FROM exa_user_sessions
		WHERE session_id = CURRENT_SESSION
	`)
	s.Equal("MyTester 123", got[0][0].(string), "Correctly set client name/version")
	c.Disconnect()
}

func (s *testSuite) TestQueryTimeout() {
	conf := s.connConf()
	conf.SuppressError = true
	conf.QueryTimeout = 5 * time.Second
	c, err := Connect(conf)
	s.Nil(err, "No connection errors")
	c.Execute("OPEN SCHEMA " + s.qschema)

	// First create a function that sleeps for 3 sec
	// to simulate a long running query
	c.Execute(`
		CREATE SCRIPT sleep(sec) AS
		local ntime = os.time() + sec
		repeat until os.time() > ntime
		exit({rows_affected=123})
	`)

	// Now run the script and verify that it didn't timeout
	got, _ := c.Execute(`EXECUTE SCRIPT sleep(1)`)
	s.Equal(int64(123), got, "Did not time out")

	// Now run the script longer and verify that it aborted
	got, err = c.Execute(`EXECUTE SCRIPT sleep(10)`)
	if s.Error(err) {
		s.Contains(err.Error(), "Server terminated statement", "Got error")
	}
	s.Equal(int64(0), got, "Timed out")

	// No need to disconnect because the server killed the connection
}

func (s *testSuite) TestConnectTimeout() {
	conf := s.connConf()
	conf.SuppressError = true
	conf.ConnectTimeout = 5 * time.Second
	// To test this properly you need to set the EXA_TIMEOUT_HOST ENV
	// to a host+port that will result in a hanging connection.
	env := os.Getenv("EXA_TIMEOUT_HOST")
	if env == "" {
		s.T().Skip("EXA_TIMEOUT_HOST must be set to 'host:port' in order for TestConnectTimeout to run.")
	}
	parts := strings.Split(env, ":")
	conf.Host = parts[0]
	port, _ := strconv.ParseUint(parts[1], 10, 64)
	conf.Port = uint16(port)

	timeIn := time.Now()
	_, err := Connect(conf)
	if s.Error(err) {
		s.Contains(err.Error(), "Unable to connect", "Got error")
	}
	s.Less(time.Since(timeIn).Seconds(), conf.ConnectTimeout.Seconds()+1, "It timed out correctly")
	s.Greater(time.Since(timeIn).Seconds(), conf.ConnectTimeout.Seconds()-1, "It did hang")
}

func (s *testSuite) TestConnSuppressError() {
	conf := s.connConf()
	output := &bytes.Buffer{}
	logger := customTestLogger("error")
	logger.SetOutput(output)
	conf.Logger = logger

	// First allow error logging
	conf.SuppressError = false
	c, _ := Connect(conf)

	c.Execute("SELECT 1")
	s.Equal(output.String(), "", "No error output")

	c.Execute("ASDF")
	s.Contains(output.String(), "syntax error", "Got error output")
	c.Disconnect()

	// Now suppress the error logging
	output.Reset()
	conf.SuppressError = true
	c, _ = Connect(conf)

	c.Execute("SELECT 1")
	s.Equal(output.String(), "", "No error output")

	c.Execute("ASDF")
	s.Equal(output.String(), "", "Still no error output")
	c.Disconnect()
}

func (s *testSuite) TestConnLogger() {
	conf := s.connConf()

	// First test that it works with no logger at all
	conf.Logger = nil
	c, err := Connect(conf)
	s.Nil(err, "No error")
	got, err := c.FetchSlice("SELECT 123")
	s.Nil(err, "Still no error")
	s.Equal(got[0][0].(float64), float64(123), "Everything OK")
	got, err = c.FetchSlice("ASDF")
	s.NotNil(err, "Got error")
	s.Nil(got, "No results")
	c.Disconnect()

	// Now test with our own logger
	output := &bytes.Buffer{}
	logger := customTestLogger("error")
	logger.SetOutput(output)
	conf.Logger = logger

	c, err = Connect(conf)
	s.Nil(err, "No error")

	got, err = c.FetchSlice("SELECT 123")
	s.Nil(err, "Still no error")
	s.Equal(got[0][0].(float64), float64(123), "Everything OK")
	s.Equal(output.String(), "", "No error output")

	got, err = c.FetchSlice("ASDF")
	s.NotNil(err, "Got error")
	s.Nil(got, "No results")
	s.Contains(output.String(), "syntax error", "Got error output")
	output.Reset()

	// Turn on debugging
	l, _ := logrus.ParseLevel("debug")
	logger.SetLevel(l)
	got, err = c.FetchSlice("SELECT 123")
	s.Nil(err, "Still no error")
	s.Equal(got[0][0].(float64), float64(123), "Everything OK")
	s.Contains(output.String(), "Execute: SELECT 123", "Got debug output")

	c.Disconnect()
}

func (s *testSuite) TestConnCachePrepStmt() {
	conf := s.connConf()

	// First try with cache disabled
	conf.CachePrepStmts = false
	c, _ := Connect(conf)

	got, _ := c.FetchSlice("SELECT 123 FROM dual WHERE true = ?", []interface{}{true})
	s.Equal(got[0][0].(float64), float64(123), "Everything OK")
	s.Equal(c.Stats["StmtCacheLen"], 0, "Cache is empty")
	s.Equal(c.Stats["StmtCacheMiss"], 0, "Cache miss not recorded")

	c.Disconnect()

	// Then try with cache enabled
	conf.CachePrepStmts = true
	c, _ = Connect(conf)

	got, _ = c.FetchSlice("SELECT 123 FROM dual WHERE true = ?", []interface{}{true})
	s.Equal(got[0][0].(float64), float64(123), "Everything OK")
	s.Equal(c.Stats["StmtCacheLen"], 1, "Cache is not empty")
	s.Equal(c.Stats["StmtCacheMiss"], 1, "Cache miss recorded")

	got, _ = c.FetchSlice("SELECT 123 FROM dual WHERE true = ?", []interface{}{true})
	s.Equal(got[0][0].(float64), float64(123), "Everything OK")
	s.Equal(c.Stats["StmtCacheLen"], 1, "Cache is not empty")
	s.Equal(c.Stats["StmtCacheMiss"], 1, "Cache miss not recorded")

	c.Disconnect()
}

func (s *testSuite) TestConnEncryption() {
	conf := s.connConf()

	// Enable Encryption
	conf.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	c, err := Connect(conf)
	s.Nil(err, "No connection errors")

	got, _ := c.FetchSlice(`
		SELECT encrypted
		FROM exa_user_sessions
		WHERE session_id = CURRENT_SESSION
	`)
	s.Equal(true, got[0][0].(bool), "Connection is encrypted")
	c.Disconnect()

	// Disable Encryption
	conf.TLSConfig = nil
	c, err = Connect(conf)
	s.Nil(err, "No connection errors")

	got, _ = c.FetchSlice(`
		SELECT encrypted
		FROM exa_user_sessions
		WHERE session_id = CURRENT_SESSION
	`)
	s.Equal(false, got[0][0].(bool), "Connection is encrypted")
	c.Disconnect()
}

func (s *testSuite) TestConnErrors() {
	// Connection error
	conf := s.connConf()
	conf.SuppressError = true
	conf.Host = "-1"
	c, err := Connect(conf)
	s.Nil(c)
	if s.Error(err) {
		s.Contains(err.Error(), "Unable to connect")
	}

	// Authentication error
	conf = s.connConf()
	conf.SuppressError = true
	conf.Username = ""
	c, err = Connect(conf)
	s.Nil(c)
	if s.Error(err) {
		s.Contains(err.Error(), "Unable to login")
	}
}

// This also tests GetSessionAttr
func (s *testSuite) TestAutoCommit() {
	exa := s.exaConn

	got, _ := exa.GetSessionAttr()
	s.Equal(true, got.Autocommit, "Autocommit defaults to true")

	exa.DisableAutoCommit()
	got, _ = exa.GetSessionAttr()
	s.Equal(false, got.Autocommit, "Autocommit is disabled")

	exa.FetchSlice("SELECT 1")
	got, _ = exa.GetSessionAttr()
	s.Equal(false, got.Autocommit, "Autocommit still disabled")

	exa.EnableAutoCommit()
	got, _ = exa.GetSessionAttr()
	s.Equal(true, got.Autocommit, "Autocommit is enabled")

	exa.FetchSlice("SELECT 1")
	got, _ = exa.GetSessionAttr()
	s.Equal(true, got.Autocommit, "Autocommit still enabled")
}

func (s *testSuite) TestCommitAndRollback() {
	exa := s.exaConn
	exa.DisableAutoCommit()
	exa.Execute("CREATE TABLE foo ( id INT )")
	exa.Commit()

	exa.Execute("INSERT INTO foo VALUES (123)")
	got, _ := exa.FetchSlice("SELECT id FROM foo")
	s.Len(got, 1, "Data is there")

	exa.Rollback()
	got, _ = exa.FetchSlice("SELECT id FROM foo")
	s.Len(got, 0, "The data is gone")

	exa.Execute("INSERT INTO foo VALUES (123)")
	got, _ = exa.FetchSlice("SELECT id FROM foo")
	s.Len(got, 1, "The data is back again")

	exa.Commit()
	exa.Rollback()
	got, _ = exa.FetchSlice("SELECT id FROM foo")
	s.Len(got, 1, "Still there after rollback because of prior commit")
}

func (s *testSuite) TestSessionID() {
	exa := s.exaConn
	sesh, _ := exa.FetchSlice("SELECT CURRENT_SESSION")
	s.Equal(sesh[0][0].(string), fmt.Sprintf("%d", exa.SessionID), "SessionID is correct")
	s.Equal(sesh[0][0].(string), fmt.Sprintf("%d", exa.Metadata.SessionID), "SessionID in metadata is correct")
}

func (s *testSuite) TestExecute() {
	exa := s.exaConn
	exa.Conf.SuppressError = true
	exa.Execute("CREATE TABLE foo ( id INT, val CHAR(1) )")
	exa.Commit()

	// Generate an error
	got, err := exa.Execute("ASDF")
	if s.Error(err) {
		s.Contains(err.Error(), "syntax error")
	}
	s.Equal(int64(0), got)

	// Successful, no binds
	got, err = exa.Execute("INSERT INTO foo VALUES (1,'a'),(2,'b'),(3,'c')")
	s.Nil(err)
	s.Equal(int64(3), got)

	// With []interface{} binds
	got, err = exa.Execute("INSERT INTO foo VALUES (?,?)", []interface{}{1, "a"})
	s.Nil(err)
	s.Equal(int64(1), got)

	// With [][]interface{} binds
	got, err = exa.Execute("INSERT INTO foo VALUES (?,?)", [][]interface{}{{1, "a"}, {2, "b"}})
	s.Nil(err)
	s.Equal(int64(2), got)

	// With default schema
	exa.Execute("OPEN SCHEMA sys")
	got, err = exa.Execute("INSERT INTO foo VALUES (1,'a')") // This should fail
	if s.Error(err) {
		s.Contains(err.Error(), "not found")
	}
	got, err = exa.Execute("INSERT INTO foo VALUES (1,'a')", nil, s.schema) // This should work
	s.Nil(err)
	s.Equal(int64(1), got)

	// With column data types
	got, err = exa.Execute(
		"INSERT INTO foo VALUES (?,?)",
		[]interface{}{1, "a"},
		nil,
		[]DataType{
			{Type: "DECIMAL", Precision: 10},
			{Type: "CHAR", Size: 1},
		},
	)
	s.Nil(err)
	s.Equal(int64(1), got)

	// With isColumnar flag
	got, err = exa.Execute(
		"INSERT INTO foo VALUES (?,?)",
		[][]interface{}{{1, 2, 3}, {"a", "b", "c"}},
		nil, nil, false,
	) // This should fail because the data is columnar and the flag is false
	if s.Error(err) {
		s.Contains(err.Error(), "number of column metadata objects is not the same")
	}

	got, err = exa.Execute(
		"INSERT INTO foo VALUES (?,?)",
		[][]interface{}{{1, 2, 3}, {"a", "b", "c"}},
		nil, nil, true,
	) // This should work
	s.Nil(err)
	s.Equal(int64(3), got)
}

func (s *testSuite) TestFetchChan() {
	exa := s.exaConn
	exa.Conf.SuppressError = true
	exa.Execute("CREATE TABLE foo ( id INT, val CHAR(1) )")
	exa.Execute(
		"INSERT INTO foo VALUES (?,?)",
		[][]interface{}{{1, 2, 3}, {"a", "b", "c"}},
		nil, nil, true,
	)

	// First an error
	got, err := exa.FetchChan("ASDF")
	if s.Error(err) {
		s.Contains(err.Error(), "syntax error")
	}
	s.Nil(got)

	// Successful, no binds
	got, err = exa.FetchChan("SELECT * FROM foo WHERE id < 3 ORDER BY id")
	if s.NoError(err) {
		var res [][]interface{}
		for row := range got {
			res = append(res, row)
		}
		expect := [][]interface{}{
			{float64(1), "a"},
			{float64(2), "b"},
		}
		s.Equal(expect, res)
	}

	// Successful, with binds
	got, err = exa.FetchChan("SELECT * FROM foo WHERE id < ? ORDER BY id", []interface{}{3})
	if s.NoError(err) {
		var res [][]interface{}
		for row := range got {
			res = append(res, row)
		}
		expect := [][]interface{}{
			{float64(1), "a"},
			{float64(2), "b"},
		}
		s.Equal(expect, res)
	}

	// Successful, with schema
	_, err = exa.Execute("OPEN SCHEMA sys")
	s.Nil(err)
	got, err = exa.FetchChan("SELECT * FROM foo WHERE id < ? ORDER BY id", []interface{}{3}, s.schema)
	if s.NoError(err) {
		var res [][]interface{}
		for row := range got {
			res = append(res, row)
		}
		expect := [][]interface{}{
			{float64(1), "a"},
			{float64(2), "b"},
		}
		s.Equal(expect, res)
	}
}

func (s *testSuite) TestFetchSlice() {
	exa := s.exaConn
	exa.Execute("CREATE TABLE foo ( id INT, val CHAR(1) )")
	exa.Execute(
		"INSERT INTO foo VALUES (?,?)",
		[][]interface{}{{1, 2, 3}, {"a", "b", "c"}},
		nil, nil, true,
	)

	// First an error
	exa.Conf.SuppressError = true
	got, err := exa.FetchSlice("ASDF")
	if s.Error(err) {
		s.Contains(err.Error(), "syntax error")
	}
	s.Nil(got)

	got, err = exa.FetchSlice("SELECT * FROM foo WHERE id < 3 ORDER BY id")
	if s.NoError(err) {
		expect := [][]interface{}{
			{float64(1), "a"},
			{float64(2), "b"},
		}
		s.Equal(expect, got)
	}
}

func (s *testSuite) TestSetTimeout() {
	conf := s.connConf()
	conf.QueryTimeout = 5 * time.Second
	c, err := Connect(conf)
	s.Nil(err)
	attr, err := c.GetSessionAttr()
	s.Nil(err)
	s.Equal(uint32(5), attr.QueryTimeout)

	err = c.SetTimeout(10)
	s.Nil(err)
	attr, err = c.GetSessionAttr()
	s.Nil(err)
	s.Equal(uint32(10), attr.QueryTimeout)
}
