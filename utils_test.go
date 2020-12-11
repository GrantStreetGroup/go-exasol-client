package exasol

func (s *testSuite) TestQuoteIdent() {
	exa := s.exaConn
	s.Equal("[test]", exa.QuoteIdent("[test]"), "Already quoted")
	s.Equal(`"test"`, exa.QuoteIdent(`"test"`), "Already quoted")
	s.Equal("[SELECT]", exa.QuoteIdent("SELect"), "Keyword")
	s.Equal("[select]", exa.QuoteIdent("SELect", true), "Keyword")
	s.Equal("[-MYID]", exa.QuoteIdent("-myid"), "Special characters")
	s.Equal("okAY", exa.QuoteIdent("okAY"), "Default")
}

func (s *testSuite) TestQuoteStr() {
	s.Equal("my''str", QuoteStr("my'str"))
}

func (s *testSuite) TestTranspose() {
	data := [][]interface{}{{1, "a"}, {2, "b"}, {3, "c"}}
	expect := [][]interface{}{{1, 2, 3}, {"a", "b", "c"}}
	s.Equal(expect, Transpose(data))
}
