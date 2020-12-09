package exasol

import "fmt"

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

func (s *testSuite) TestSessionID() {
	exa := s.exaConn
	sesh, _ := exa.FetchSlice("SELECT CURRENT_SESSION")
	s.Equal(sesh[0][0].(string), fmt.Sprintf("%d", exa.SessionID), "SessionID is correct")
}
