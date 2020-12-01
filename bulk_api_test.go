package exasol

import (
	"bytes"
	"fmt"
)

func (s *testSuite) TestStreamSelect() {
	s.execute(`CREATE TABLE foo ( id INT, val CLOB )`)
	s.execute(`INSERT INTO foo VALUES (1,'a'),(2,'b'),(3,'c')`)
	rows := s.exaConn.StreamSelect(s.schema, "FOO")

	var csv string
	for d := range rows.Data {
		csv += string(d)
	}
	rows.Close()

	s.Equal("1,a\n2,b\n3,c\n", csv, "Streamed a select")
}

func (s *testSuite) TestStreamQuery() {
	s.execute(`CREATE TABLE foo ( id INT, val INT )`)
	s.execute(`INSERT INTO foo SELECT row_number() over() c, local.c FROM dual CONNECT BY LEVEL <= 3e5`)
	rows := s.exaConn.StreamQuery(fmt.Sprintf(`
		EXPORT ( SELECT t.id, t.val, NULL FROM %s.foo AS t ORDER BY t.id DESC NULLS LAST)
		INTO CSV AT '%%s' FILE 'data.csv'
		DELIMIT = NEVER
		COLUMN SEPARATOR = '0x00'
	`, s.schema))

	var csv string
	for d := range rows.Data {
		csv += string(d)
		rows.Pool.Put(d)
	}
	rows.Close()

	s.Equal("300000\x00300000\x00\n29999", csv[:20], "Beginning ok")
	s.Equal("2\x002\x00\n1\x001\x00\n", csv[len(csv)-10:], "End ok")
}

func (s *testSuite) TestStreamInsert() {
	s.execute(`CREATE TABLE foo ( id INT, val VARCHAR(10) )`)
	numRows := 1000
	data := make(chan []byte, numRows)
	for i := 1; i <= numRows; i++ {
		data <- []byte(fmt.Sprintf("%d,'%d'\n", i, i+10))
	}
	close(data)
	s.exaConn.StreamInsert(s.schema, "foo", data)

	got := s.fetch(`SELECT COUNT(*), MIN(id), MAX(id) FROM foo`)
	expect := [][]interface{}{{float64(numRows), float64(1), float64(numRows)}}
	s.Equal(expect, got, "Correctly stream-inserted")
}

func (s *testSuite) TestBulkExecuteFailure() {
	s.exaConn.Conf.SuppressError = true
	err := s.exaConn.BulkExecute("bar %s cow", bytes.NewBufferString("foo"))
	s.exaConn.Conf.SuppressError = false
	s.NotNil(err, "Correctly failed")
}
