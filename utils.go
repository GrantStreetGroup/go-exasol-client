/*
    AUTHOR

	Grant Street Group <developers@grantstreet.com>

	COPYRIGHT AND LICENSE

	This software is Copyright (c) 2019 by Grant Street Group.
	This is free software, licensed under:
	    MIT License
*/

/*--- Various utility routines ---*/

package exasol

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
)

var keywordLock sync.RWMutex
var keywords map[string]bool

/*--- Public Interface ---*/

// The optional second argument to QuoteIdent is for backwards compatibility.
// By default if an identifier name is an unquoted Exasol keyword it is
// uppercased before quoting. If you would rather it be lowercased then
// pass in "true" for the second argument.

func (c *Conn) QuoteIdent(ident string, args ...interface{}) string {
	var lowerKeywords bool
	if len(args) > 0 && args[0] != nil {
		switch b := args[0].(type) {
		case bool:
			lowerKeywords = b
		default:
			c.error("QuoteIdent's 2nd param (lowerKeywords) must be boolean")
		}
	}

	if regexp.MustCompile(`^(\[|")`).MatchString(ident) {
		// Return if already quoted
		return ident
	}

	if keywords == nil {
		keywordLock.Lock()
		if keywords == nil {
			kw := map[string]bool{}
			sql := "SELECT LOWER(keyword) FROM sys.exa_sql_keywords WHERE reserved"
			kwRes, _ := c.FetchChan(sql)
			for col := range kwRes {
				kw[col[0].(string)] = true
			}
			keywords = kw
		}
		keywordLock.Unlock()
	}
	_, isKeyword := keywords[strings.ToLower(ident)]
	if isKeyword {
		if lowerKeywords {
			return fmt.Sprintf(`[%s]`, strings.ToLower(ident))
		} else {
			return fmt.Sprintf(`[%s]`, strings.ToUpper(ident))
		}
	} else if regexp.MustCompile(`^[^A-Za-z]`).MatchString(ident) ||
		regexp.MustCompile(`[^A-Za-z0-9_]`).MatchString(ident) {
		// From docs "...a regular identifier may start with letters of the set
		//  {a-z, A-Z} and may further contain letters of set {a-z, A-Z, 0-9,_}
		// For quoted identifiers any characters can be contained within
		// the quotation marks except the dot ('.')
		ident = regexp.MustCompile(`\.`).ReplaceAllString(ident, "_")
		return fmt.Sprintf(`[%s]`, strings.ToUpper(ident))
	}
	return ident
}

func QuoteStr(str string) string {
	return regexp.MustCompile("'").ReplaceAllString(str, "''")
}

func Transpose(matrix [][]interface{}) [][]interface{} {
	numRows := len(matrix)
	numCols := len(matrix[0])
	ret := make([][]interface{}, numCols)

	for x, _ := range ret {
		ret[x] = make([]interface{}, numRows)
	}
	for y, s := range matrix {
		for x, e := range s {
			ret[x][y] = e
		}
	}
	return ret
}

/*--- Private Routines ---*/

func (c *Conn) error(text string) error {
	err := errors.New(text)
	if !c.Conf.SuppressError {
		c.log.Error(err)
	}
	return err
}

func (c *Conn) errorf(format string, args ...interface{}) error {
	err := fmt.Errorf(format, args...)
	if c.Conf.SuppressError == false {
		c.log.Error(err)
	}
	return err
}

func transposeToChan(ch chan<- []interface{}, matrix [][]interface{}) {
	// matrix is columnar ... this transposes it to rowular
	for row := range matrix[0] {
		ret := make([]interface{}, len(matrix))
		for col := range matrix {
			ret[col] = matrix[col][row]
		}
		ch <- ret
	}
}
