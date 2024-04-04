/*
	By default this test suite assumes there is a local Exasol instance
	listening on port 8563 and with a default sys password. You can
	override this via --host, --port, and --pass test arguments.

	We recommend using an Exasol docker container for this:
		https://github.com/exasol/docker-db

	Run tests via: go test -v -args -testify.m pattern

	The routines in this file are shared by all the test files.
	There aren't any actual tests in this file.
*/
package exasol

import (
	"crypto/tls"
	"flag"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

var testHost = flag.String("host", "127.0.0.1", "Exasol hostname")
var testPort = flag.Int("port", 8563, "Exasol port")
var testPass = flag.String("pass", "exasol", "Exasol SYS password")
var testLoglevel = flag.String("loglevel", "warning", "Output loglevel")

type testSuite struct {
	suite.Suite
	exaConn *Conn
	log     *logrus.Logger
	schema  string
	qschema string
}

func TestExasolClient(t *testing.T) {
	s := initTestSuite()
	s.connectExasol()
	defer s.exaConn.Disconnect()
	suite.Run(t, s)
}

func initTestSuite() *testSuite {
	s := new(testSuite)
	s.log = customTestLogger(*testLoglevel)
	s.schema = "test"
	s.qschema = "[test]"
	return s
}

func (s *testSuite) connConf() ConnConf {
	return ConnConf{
		Host:      *testHost,
		Port:      uint16(*testPort),
		Username:  "SYS",
		Password:  *testPass,
		TLSConfig: &tls.Config{InsecureSkipVerify: true},
	}
}

func (s *testSuite) connectExasol() {
	var err error
	s.exaConn, err = Connect(s.connConf())
	if err != nil {
		logrus.Fatal(err)
	}
}

func (s *testSuite) SetupTest() {
	if s.exaConn != nil {
		s.execute("DROP SCHEMA IF EXISTS " + s.qschema + " CASCADE")
		s.execute("CREATE SCHEMA " + s.qschema)
	}
}

func (s *testSuite) TearDownTest() {
	if s.exaConn != nil {
		s.exaConn.Rollback()
	}
}

func (s *testSuite) execute(args ...string) {
	for _, arg := range args {
		_, err := s.exaConn.Execute(arg)
		if !s.exaConn.Conf.SuppressError {
			s.NoError(err, "Unable to execute SQL")
		}
	}
}

func (s *testSuite) fetch(sql string) [][]interface{} {
	data, err := s.exaConn.FetchSlice(sql)
	if !s.exaConn.Conf.SuppressError {
		s.NoError(err, "Unable to execute SQL")
	}
	return data
}

func customTestLogger(logLevelStr string) *logrus.Logger {
	log := logrus.New()
	l, _ := logrus.ParseLevel(logLevelStr)
	log.SetLevel(l)
	log.WithFields(logrus.Fields{"test": "123"})
	return log
}
