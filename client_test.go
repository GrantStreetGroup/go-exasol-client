package exasol

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net/url"
	"os"
	"regexp"
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
CREATE OR REPLACE PYTHON3 SCALAR SCRIPT sleep("d" INTEGER) RETURNS INTEGER AS
import subprocess
def run(ctx):
	subprocess.check_output("sleep " + str(ctx.d), shell=True)
	return 123
	`)

	// Now run the script and verify that it didn't timeout
	got, err := c.FetchSlice(`SELECT sleep(1)`)
	s.Nil(err, "No query errors")
	s.Equal(123.0, got[0][0].(float64), "Did not time out")

	// Now run the script longer and verify that it aborted
	_, err = c.FetchSlice(`SELECT sleep(10)`)
	if s.Error(err) {
		s.Contains(err.Error(), "timeout", "Got error")
	}

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

func (s *testSuite) TestHostRanges() {
	conf := s.connConf()
	conf.SuppressError = true // Set to false to see the random output
	conf.Host = "127.0.0.1..3"
	conf.Port = 1
	for i := 0; i < 10; i++ {
		c, err := Connect(conf)
		s.Nil(c)
		if s.Error(err) {
			s.Regexp(regexp.MustCompile(`\b127\.0\.0\.(1|2|3)\b`), err.Error())
		}
	}
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

	// Test No results at all
	got, err = exa.FetchSlice("SELECT * FROM foo WHERE FALSE")
	if s.NoError(err) {
		var exp [][]interface{}
		s.Equal(exp, got)
	}
}

func (s *testSuite) TestLargeFetch() {
	// This results in a payload > 64MB but < 1000 rows which triggers
	// result handles but still has data in the initial response
	val := strings.Repeat("x", 2000000)
	payload := [][]interface{}{{}, {}}
	for i := 0; i < 100; i++ {
		payload[0] = append(payload[0], float64(i))
		payload[1] = append(payload[1], val)
	}
	exa := s.exaConn
	exa.Execute("CREATE TABLE foo ( id INT, val VARCHAR(2000000) )")
	exa.Execute("INSERT INTO foo VALUES (?,?)", payload, nil, nil, true)

	got, err := exa.FetchSlice("SELECT * FROM foo ORDER BY id")
	if s.NoError(err) {
		s.Equal(Transpose(payload), got)
	}

	// This results in a payload < 64MB but > 1000 rows which triggers
	// result handles but and no data in the initial response
	payload = [][]interface{}{{}, {}}
	for i := 0; i < 2500; i++ {
		payload[0] = append(payload[0], float64(i))
		payload[1] = append(payload[1], "a")
	}
	exa.Execute("CREATE OR REPLACE TABLE foo ( id INT, val CHAR(1) )")
	exa.Execute("INSERT INTO foo VALUES (?,?)", payload, nil, nil, true)

	got, err = exa.FetchSlice("SELECT * FROM foo ORDER BY id")
	if s.NoError(err) {
		s.Equal(Transpose(payload), got)
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

func (s *testSuite) TestHashTypeInsert() {
	// This insert fails with Exasol v8 + websocket API v1
	exa := s.exaConn
	exa.Execute("CREATE TABLE foo (ht HASHTYPE)")
	got, err := exa.Execute("INSERT INTO foo VALUES (?)", []interface{}{"00000000000000000000000000000000"})
	s.Nil(err)
	s.Equal(int64(1), got)
}

type testWSHandler struct{}

func (wsh *testWSHandler) Connect(u url.URL, s *tls.Config, t time.Duration) error {
	return fmt.Errorf("Connecting in test handler")
}
func (wsh *testWSHandler) WriteJSON(req interface{}) error { return nil }
func (wsh *testWSHandler) ReadJSON(resp interface{}) error { return nil }
func (wsh *testWSHandler) EnableCompression(e bool)        {}
func (wsh *testWSHandler) Close()                          {}

func (s *testSuite) TestWSHandler() {
	conf := s.connConf()
	conf.SuppressError = true
	conf.WSHandler = &testWSHandler{}
	_, err := Connect(conf)
	if s.Error(err) {
		s.Contains(err.Error(), "Connecting in test handler", "Got error")
	}
}
