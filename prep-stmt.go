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

type prepStmt struct {
	sth      int
	columns  []column
	lastUsed time.Time
}

func (c *Conn) getPrepStmt(schema, sql string) (*prepStmt, error) {
	// TODO die if the num cols/rows expected by prepared statement
	//      doesn't match the passed in data (i.e. placeholder/binds mismatch)
	//      otherwise results in lowerlevel websocket closure

	c.log.Debug("Preparing stmt for:", sql)
	psc := c.prepStmtCache
	ps := psc[sql]
	if ps == nil {
		var err error
		ps, err = c.createPrepStmt(schema, sql)
		if err != nil {
			return nil, err
		}
		if c.Conf.CachePrepStmts {
			psc[sql] = ps
			c.Stats["StmtCacheLen"] = len(psc)
			c.Stats["StmtCacheMiss"]++
		}
	}
	ps.lastUsed = time.Now()

	// Prune the prep stmt cache. I don't know how necessary it is
	// but I saw something on the site about Exasol
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
	sthReq := &createPrepStmtReq{
		Command:    "createPreparedStatement",
		Attributes: &Attributes{CurrentSchema: schema},
		SqlText:    sql,
	}
	sthRes := &createPrepStmtRes{}
	err := c.send(sthReq, sthRes)
	if err != nil {
		return nil, err
	}

	sth := sthRes.ResponseData.StatementHandle
	cols := sthRes.ResponseData.ParameterData.Columns
	return &prepStmt{sth, cols, time.Now()}, nil
}

func (c *Conn) closePrepStmt(sth int) error {
	c.log.Debug("Closing stmt handle ", sth)
	closeReq := &closePrepStmt{
		Command:         "closePreparedStatement",
		StatementHandle: int(sth),
	}
	err := c.send(closeReq, &response{})
	if err != nil {
		return c.errorf("Unable to closePrepStmt: %s", err)
	}
	return nil
}
