/*
    AUTHOR

	Grant Street Group <developers@grantstreet.com>

	COPYRIGHT AND LICENSE

	This software is Copyright (c) 2019 by Grant Street Group.
	This is free software, licensed under:
	    MIT License
*/

package exasol

import (
	"sort"
	"time"
)

type createPrepStmtJSON struct {
	Command    string   `json:"command"`
	Attributes attrJSON `json:"attributes,omitempty"`
	SQLtext    string   `json:"sqlText"`
}

type execPrepStmtJSON struct {
	Command         string          `json:"command"`
	Attributes      attrJSON        `json:"attributes,omitempty"`
	StatementHandle int             `json:"statementHandle"`
	NumColumns      int             `json:"numColumns"`
	NumRows         int             `json:"numRows"`
	Columns         []interface{}   `json:"columns"`
	Data            [][]interface{} `json:"data"`
}

type closePrepStmtJSON struct {
	Command         string   `json:"command"`
	Attributes      attrJSON `json:"attributes,omitempty"`
	StatementHandle int      `json:"statementHandle"`
}

type prepStmt struct {
	sth        float64
	lastUsed   time.Time
	columnDefs []interface{}
}

func (c *Conn) getPrepStmt(schema, sql string) (*prepStmt, error) {
	// TODO die if the num cols/rows expected by prepared statement
	//      doesn't match the passed in data (i.e. placeholder/binds mismatch)
	//      otherwise results in lowerlevel websocket closure

	log.Info("EXA: Preparing stmt for:", sql)
	psc := c.prepStmtCache
	ps := psc[sql]
	if ps == nil {
		var err error
		ps, err = c.createPrepStmt(schema, sql)
		if err != nil {
			return nil, err
		}
		psc[sql] = ps
		c.Stats["StmtCacheLen"] = len(psc)
		c.Stats["StmtCacheMiss"]++
	}
	ps.lastUsed = time.Now()

	// Prune the prep stmt cache. I don't know how necessary it is
	// but I saw something on their site about Exasol
	// being unhappy if there are thousands of open statements.
	if len(psc) > 1000 {
		sortedStmts := make([]string, len(psc))
		i := 0
		for sql := range psc {
			sortedStmts[i] = sql
			i++
		}
		sort.Slice(sortedStmts, func(i, j int) bool {
			return psc[sortedStmts[i]].lastUsed.Before(psc[sortedStmts[j]].lastUsed)
		})
		leastUsed := sortedStmts[0]
		c.closePrepStmt(psc[leastUsed].sth)
		delete(psc, leastUsed)
	}

	return ps, nil
}

func (c *Conn) createPrepStmt(schema string, sql string) (*prepStmt, error) {
	sthReq := &createPrepStmtJSON{
		Command:    "createPreparedStatement",
		Attributes: attrJSON{CurrentSchema: schema},
		SQLtext:    sql,
	}
	resp, err := c.send(sthReq)
	if err != nil {
		return nil, err
	}

	sth := resp["statementHandle"].(float64)
	paramData := resp["parameterData"].(map[string]interface{})
	columnDefs := paramData["columns"].([]interface{})
	log.Info("EXA: Got stmt handle ", sth)
	return &prepStmt{sth, time.Now(), columnDefs}, nil
}

func (c *Conn) closePrepStmt(sth float64) {
	log.Info("EXA: Closing stmt handle ", sth)
	closeReq := &closePrepStmtJSON{
		Command:         "closePreparedStatement",
		StatementHandle: int(sth),
	}
	c.send(closeReq)
}
