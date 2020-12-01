package exasol

import ()

func (s *testSuite) TestAutoCommit() {
	exa := s.exaConn

	got, _ := exa.GetSessionAttr()
	s.Equal(true, got["autocommit"], "Autocommit defaults to true")

	exa.DisableAutoCommit()
	got, _ = exa.GetSessionAttr()
	s.Equal(false, got["autocommit"], "Autocommit is disabled")

	exa.FetchSlice("SELECT 1")
	got, _ = exa.GetSessionAttr()
	s.Equal(false, got["autocommit"], "Autocommit still disabled")

	exa.EnableAutoCommit()
	got, _ = exa.GetSessionAttr()
	s.Equal(true, got["autocommit"], "Autocommit is enabled")

	exa.FetchSlice("SELECT 1")
	got, _ = exa.GetSessionAttr()
	s.Equal(true, got["autocommit"], "Autocommit still enabled")
}
