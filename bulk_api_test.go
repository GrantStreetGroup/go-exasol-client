package exasol

import (
	"bytes"
	"fmt"
)

func (s *testSuite) TestBulkInsert() {
	exa := s.exaConn
	exa.Execute("CREATE TABLE foo ( id INT, val CHAR(1) )")

	data := bytes.NewBufferString("1,a\n2,b\n3,c")
	s.exaConn.Conf.SuppressError = true
	// Should fail
	err := exa.BulkInsert(s.qschema, "ASDF", data)
	if s.Error(err) {
		s.Contains(err.Error(), "ASDF")
	}

	// Should succeed
	err = exa.BulkInsert(s.qschema, "FOO", data)
	s.Nil(err)

	got, err := exa.FetchSlice("SELECT * FROM foo ORDER BY id")
	if s.NoError(err) {
		expect := [][]interface{}{
			{float64(1), "a"},
			{float64(2), "b"},
			{float64(3), "c"},
		}
		s.Equal(expect, got)
	}
}

func (s *testSuite) TestBulkExecute() {
	exa := s.exaConn
	exa.Execute("CREATE TABLE foo ( id INT, val CHAR(1) )")

	data := bytes.NewBufferString("1,\"a\"\n2,\"b\"\n3,\"c\"")
	s.exaConn.Conf.SuppressError = true
	// Should fail
	err := exa.BulkExecute("ASDF", data)
	if s.Error(err) {
		s.Contains(err.Error(), "ASDF")
	}

	// Should succeed
	err = exa.BulkExecute("IMPORT INTO [test].FOO FROM CSV AT '%s' FILE 'data.csv'", data)
	s.Nil(err)

	got, err := exa.FetchSlice("SELECT * FROM foo ORDER BY id")
	if s.NoError(err) {
		expect := [][]interface{}{
			{float64(1), "a"},
			{float64(2), "b"},
			{float64(3), "c"},
		}
		s.Equal(expect, got)
	}
}

func (s *testSuite) TestBulkSelect() {
	exa := s.exaConn
	exa.Execute("CREATE TABLE foo ( id INT, val CHAR(1) )")
	exa.Execute("INSERT INTO foo VALUES (1,'a'),(2,'b'),(3,'c')")

	data := &bytes.Buffer{}
	s.exaConn.Conf.SuppressError = true

	// Should fail
	err := exa.BulkSelect(s.qschema, "ASDF", data)
	if s.Error(err) {
		s.Contains(err.Error(), "not found")
	}

	// Should succeed
	err = exa.BulkSelect(s.qschema, "FOO", data)
	if s.NoError(err) {
		s.Equal("1,a\n2,b\n3,c\n", data.String())
	}
}

func (s *testSuite) TestBulkQuery() {
	exa := s.exaConn
	exa.Execute("CREATE TABLE foo ( id INT, val CHAR(1) )")
	exa.Execute("INSERT INTO foo VALUES (1,'a'),(2,'b'),(3,'c')")

	data := &bytes.Buffer{}
	s.exaConn.Conf.SuppressError = true
	// Should fail
	err := exa.BulkQuery("ASDF", data)
	if s.Error(err) {
		s.Contains(err.Error(), "syntax error")
	}

	// Should succeed
	err = exa.BulkQuery(`
		EXPORT (
			SELECT id, val
			FROM foo
			ORDER BY id
		) INTO CSV AT '%s'
		  FILE 'data.csv'
	`, data)
	if s.NoError(err) {
		s.Equal("1,a\n2,b\n3,c\n", data.String())
	}
}

func (s *testSuite) TestStreamInsert() {
	s.execute(`CREATE TABLE foo ( id INT, val VARCHAR(10) )`)
	numRows := 1000
	data := make(chan []byte, numRows)
	for i := 1; i <= numRows; i++ {
		data <- []byte(fmt.Sprintf("%d,'%d'\n", i, i+10))
	}
	close(data)

	// Should fail
	s.exaConn.Conf.SuppressError = true
	err := s.exaConn.StreamInsert(s.qschema, "asdf", data)
	if s.Error(err) {
		s.Contains(err.Error(), "not found")
	}

	// Should succeed
	err = s.exaConn.StreamInsert(s.qschema, "foo", data)
	s.Nil(err)
	got := s.fetch(`SELECT COUNT(*), MIN(id), MAX(id) FROM foo`)
	expect := [][]interface{}{{float64(numRows), float64(1), float64(numRows)}}
	s.Equal(expect, got, "Correctly stream-inserted")
}

func (s *testSuite) TestStreamExecute() {
	s.execute(`CREATE TABLE foo ( id INT, val VARCHAR(10) )`)
	numRows := 1000
	data := make(chan []byte, numRows)
	for i := 1; i <= numRows; i++ {
		data <- []byte(fmt.Sprintf("%d,'%d'\n", i, i+10))
	}
	close(data)

	// Should fail
	s.exaConn.Conf.SuppressError = true
	err := s.exaConn.StreamExecute(`ASDF`, data)
	if s.Error(err) {
		s.Contains(err.Error(), "syntax error")
	}

	// Should succeed
	err = s.exaConn.StreamExecute("IMPORT INTO [test].FOO FROM CSV AT '%s' FILE 'data.csv'", data)
	s.Nil(err)
	got := s.fetch(`SELECT COUNT(*), MIN(id), MAX(id) FROM foo`)
	expect := [][]interface{}{{float64(numRows), float64(1), float64(numRows)}}
	s.Equal(expect, got, "Correctly stream-inserted")
}

func (s *testSuite) TestStreamSelect() {
	s.execute(`CREATE TABLE foo ( id INT, val CLOB )`)
	s.execute(`INSERT INTO foo VALUES (1,'a'),(2,'b'),(3,'c')`)

	// Should fail
	s.exaConn.Conf.SuppressError = true
	rows := s.exaConn.StreamSelect(s.qschema, "asdf")
	var csv string
	for d := range rows.Data {
		csv += string(d)
	}
	if s.Error(rows.Error) {
		s.Contains(rows.Error.Error(), "not found")
	}
	s.Equal("", csv, "Nothing streamed")
	s.Equal(int64(0), rows.BytesRead)

	// Should succeed
	rows = s.exaConn.StreamSelect(s.qschema, "FOO")
	s.Nil(rows.Error)

	for d := range rows.Data {
		csv += string(d)
	}
	rows.Close()

	s.Equal("1,a\n2,b\n3,c\n", csv, "Streamed a select")
	s.Equal(int64(12), rows.BytesRead)
}

func (s *testSuite) TestStreamQuery() {
	s.execute(`CREATE TABLE foo ( id INT, val INT )`)
	// Inserts 300K rows
	s.execute(`INSERT INTO foo SELECT row_number() over() c, local.c FROM dual CONNECT BY LEVEL <= 3e5`)

	// Should fail
	s.exaConn.Conf.SuppressError = true
	rows := s.exaConn.StreamQuery("asdf")
	var csv string
	for d := range rows.Data {
		csv += string(d)
	}
	if s.Error(rows.Error) {
		s.Contains(rows.Error.Error(), "syntax error")
	}
	s.Equal("", csv, "Nothing streamed")
	s.Equal(int64(0), rows.BytesRead)

	// Should succeed
	rows = s.exaConn.StreamQuery(fmt.Sprintf(`
		EXPORT ( SELECT t.id, t.val, NULL FROM %s.foo AS t ORDER BY t.id DESC NULLS LAST)
		INTO CSV AT '%%s' FILE 'data.csv'
		DELIMIT = NEVER
		COLUMN SEPARATOR = '0x00'
	`, s.qschema))

	for d := range rows.Data {
		csv += string(d)
		rows.Pool.Put(d)
	}
	rows.Close()

	s.Equal("300000\x00300000\x00\n29999", csv[:20], "Beginning ok")
	s.Equal("2\x002\x00\n1\x001\x00\n", csv[len(csv)-10:], "End ok")
	s.Equal(int64(4277790), rows.BytesRead)
}
