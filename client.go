/*
	This is a database interface library using EXASOL's websocket API
    https://github.com/exasol/websocket-api/blob/master/WebsocketAPI.md

	TODOs:
	1) Support connection compression
	2) Support connection encryption
	3) Convert to database/sql interface
	4) Implement timeouts for all query types


	AUTHOR

	Grant Street Group <developers@grantstreet.com>

	COPYRIGHT AND LICENSE

	This software is Copyright (c) 2019 by Grant Street Group.
	This is free software, licensed under:
	    MIT License
*/

package exasol

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/big"
	"os"
	"os/user"
	"regexp"
	"runtime"
	"strconv"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/op/go-logging"
)

/*--- Public Interface ---*/

type ConnConf struct {
	Host          string
	Port          uint16
	Username      string
	Password      string
	ClientName    string
	Timeout       uint32 // In Seconds
	SuppressError bool   // Server errors are logged to Error by default
	// TODO try compressionEnabled: true
	LogLevel string
}

type Conn struct {
	Conf      ConnConf
	SessionID uint64
	Stats     map[string]int

	log           *logging.Logger
	ws            *websocket.Conn
	prepStmtCache map[string]*prepStmt
	mux           sync.Mutex
}

type DataType struct {
	Type    string `json:"type"`
	Size    int    `json:"size"`
	Prec    int    `json:"precision"`
	Scale   int    `json:"scale"`
	CharSet string `json:"characterSet,omitempty"`
}

func Connect(conf ConnConf) *Conn {

	c := &Conn{
		Conf:          conf,
		Stats:         map[string]int{},
		log:           logging.MustGetLogger("exasol"),
		prepStmtCache: map[string]*prepStmt{},
	}
	c.initLogging(conf.LogLevel)

	// Both of these are fatal if they encounter an error
	c.wsConnect()
	c.login()
	return c
}

func (c *Conn) Disconnect() {
	c.log.Notice("EXA: Disconnecting SessionID:", c.SessionID)

	for _, ps := range c.prepStmtCache {
		c.closePrepStmt(ps.sth)
	}
	_, err := c.send(&disconnectJSON{Command: "disconnect"})
	if err != nil {
		c.log.Warning("Unable to disconnect from EXASOL: ", err)
	}
	c.ws.Close()
	c.ws = nil
}

func (c *Conn) GetSessionAttr() (map[string]interface{}, error) {
	c.log.Info("EXA: Getting Attr")
	req := &sendAttrJSON{Command: "getAttributes"}
	res, err := c.send(req)
	return res, err
}

func (c *Conn) EnableAutoCommit() {
	c.log.Info("EXA: Enabling AutoCommit")
	c.send(&sendAttrJSON{
		Command:    "setAttributes",
		Attributes: attrJSON{AutoCommit: true},
	})
}

func (c *Conn) DisableAutoCommit() {
	c.log.Info("EXA: Disabling AutoCommit")
	// We have to roll our own map because attrJSON
	// needs to have AutoCommit set to omitempty which
	// causes autocommit=false not to be sent :-(
	c.send(map[string]interface{}{
		"command": "setAttributes",
		"attributes": map[string]interface{}{
			"autocommit": false,
		},
	})
}

func (c *Conn) Rollback() error {
	c.log.Info("EXA: Rolling back transaction")
	_, err := c.Execute("ROLLBACK")
	return err
}

func (c *Conn) Commit() error {
	c.log.Info("EXA: Committing transaction")
	_, err := c.Execute("COMMIT")
	return err
}

// TODO change optional args into an ExecConf struct
// Optional args are binds, default schema, colDefs, isColumnar flag
func (c *Conn) Execute(sql string, args ...interface{}) (map[string]interface{}, error) {
	var binds [][]interface{}
	if len(args) > 0 && args[0] != nil {
		// TODO make the binds optionally just []interface{}
		binds = args[0].([][]interface{})
	}
	var schema string
	if len(args) > 1 && args[1] != nil {
		schema = args[1].(string)
	}
	var dataTypes []interface{}
	if len(args) > 2 && args[2] != nil {
		dataTypes = args[2].([]interface{})
	}
	isColumnar := false // Whether or not the passed-in binds are columnar
	if len(args) > 3 && args[3] != nil {
		isColumnar = args[3].(bool)
	}

	// Just a simple execute (no prepare) if there are no binds
	if binds == nil || len(binds) == 0 ||
		binds[0] == nil || len(binds[0]) == 0 {
		c.log.Info("EXA: Execute Query: ", sql)
		req := &executeStmtJSON{
			Command:    "execute",
			Attributes: attrJSON{CurrentSchema: schema},
			SQLtext:    sql,
		}
		return c.send(req)
	}

	// Else need to send data so do a prepare + execute
	ps, err := c.getPrepStmt(schema, sql)
	if err != nil {
		return nil, err
	}

	if dataTypes != nil {
		for i := range dataTypes {
			ps.columnDefs[i].(map[string]interface{})["dataType"] = dataTypes[i]
		}
	}

	if !isColumnar {
		binds = Transpose(binds)
	}
	numCols := len(binds)
	numRows := len(binds[0])

	c.log.Infof("EXA: Executing %d x %d stmt", numCols, numRows)
	execReq := &execPrepStmtJSON{
		Command:         "executePreparedStatement",
		StatementHandle: int(ps.sth),
		NumColumns:      numCols,
		NumRows:         numRows,
		Columns:         ps.columnDefs,
		Data:            binds,
	}

	res, err := c.send(execReq)

	if err != nil &&
		regexp.MustCompile("Statement handle not found").MatchString(err.Error()) {
		// Not sure what causes this but I've seen it happen. So just try again.
		c.log.Warning("Statement handle not found:", ps.sth)
		delete(c.prepStmtCache, sql)
		ps, err := c.getPrepStmt(schema, sql)
		if err != nil {
			return nil, err
		}
		c.log.Warning("Retrying with:", ps.sth)
		execReq.StatementHandle = int(ps.sth)
		res, err = c.send(execReq)
	}

	return res, err
}

func (c *Conn) FetchChan(sql string, args ...interface{}) (<-chan []interface{}, error) {
	var binds []interface{}
	if len(args) > 0 && args[0] != nil {
		binds = args[0].([]interface{})
	}
	var schema string
	if len(args) > 1 && args[1] != nil {
		schema = args[1].(string)
	}

	response, err := c.Execute(sql, [][]interface{}{binds}, schema)
	if err != nil {
		return nil, err
	}
	if response["numResults"].(float64) != 1 {
		c.log.Fatalf("Unexpected numResults: %s", response["numResults"].(float64))
	}
	results := response["results"].([]interface{})[0].(map[string]interface{})
	if results["resultSet"] == nil {
		return nil, fmt.Errorf("Missing websocket API resultset")
	}
	rs := results["resultSet"].(map[string]interface{})

	ch := make(chan []interface{}, 1000)

	go func() {
		if rs["numRows"].(float64) == 0 {
			// Do nothing
		} else if rsh, ok := rs["resultSetHandle"].(float64); ok {
			for i := float64(0); i < rs["numRows"].(float64); {
				fetchReq := &fetchJSON{
					Command:         "fetch",
					ResultSetHandle: rsh,
					StartPosition:   i,
					NumBytes:        64 * 1024 * 1024, // Max allowed
				}
				chunk, err := c.send(fetchReq)
				if err != nil {
					c.log.Panic(err)
				}
				i += chunk["numRows"].(float64)
				transposeToChan(ch, chunk["data"].([]interface{}))
			}

			closeRSReq := &closeResultSetJSON{
				Command:          "closeResultSet",
				ResultSetHandles: []float64{rsh},
			}
			_, err = c.send(closeRSReq)
			if err != nil {
				c.log.Warning("Unable to close result set:", err)
			}
		} else {
			transposeToChan(ch, rs["data"].([]interface{}))
		}
		close(ch)
	}()

	return ch, nil
}

// For large datasets use FetchChan to avoid buffering all the data in memory
func (c *Conn) FetchSlice(sql string, args ...interface{}) (res [][]interface{}, err error) {
	resChan, err := c.FetchChan(sql, args...)
	if err != nil {
		return
	}
	for row := range resChan {
		res = append(res, row)
	}
	return
}

// Gets a sync.Mutext lock on the handle.
// Allows coordinating use of the handle across multiple Go routines
func (c *Conn) Lock()   { c.mux.Lock() }
func (c *Conn) Unlock() { c.mux.Unlock() }

/*--- Private Routines ---*/

type attrJSON struct {
	AutoCommit    bool   `json:"autocommit,omitempty"`
	CurrentSchema string `json:"currentSchema,omitempty"`
}

type loginJSON struct {
	Command         string `json:"command"`
	ProtocolVersion uint16 `json:"protocolVersion"`
}

type authJSON struct {
	Username         string `json:"username"`
	Password         string `json:"password"`
	UseCompression   bool   `json:"useCompression"`
	ClientName       string `json:"clientName,omitempty"`
	DriverName       string `json:"driverName,omitempty"`
	ClientOsUsername string `json:"clientOsUsername,omitempty"`
	ClientOs         string `json:"clientOs,omitempty"`
	// TODO specify these
	//SessionId        uint64 `json:"useCompression,omitempty"`
	//ClientLanguage   string `json:"useCompression,omitempty"`
	//ClientVersion    string `json:"useCompression,omitempty"`
	//ClientRuntime    string `json:"useCompression,omitempty"`
	Attributes attrJSON `json:"attributes,omitempty"`
}

type sendAttrJSON struct {
	Command    string   `json:"command"`
	Attributes attrJSON `json:"attributes"`
}

type disconnectJSON struct {
	Command    string   `json:"command"`
	Attributes attrJSON `json:"attributes,omitempty"`
}

type executeStmtJSON struct {
	Command    string   `json:"command"`
	Attributes attrJSON `json:"attributes,omitempty"`
	SQLtext    string   `json:"sqlText"`
}

type fetchJSON struct {
	Command    string   `json:"command"`
	Attributes attrJSON `json:"attributes,omitempty"`
	// TODO change these back to ints? fetch change select * from exacols gave strange websocket error
	ResultSetHandle float64 `json:"resultSetHandle"`
	StartPosition   float64 `json:"startPosition"`
	NumBytes        int     `json:"numBytes"`
}

type closeResultSetJSON struct {
	Command          string    `json:"command"`
	Attributes       attrJSON  `json:"attributes,omitempty"`
	ResultSetHandles []float64 `json:"resultSetHandles"`
}

func (c *Conn) initLogging(logLevelStr string) {
	if logLevelStr == "" {
		logLevelStr = "error"
	}
	logLevel, err := logging.LogLevel(logLevelStr)
	if err != nil {
		c.log.Fatal("Unrecognized log level", err)
	}
	logFormat := logging.MustStringFormatter(
		"%{color}%{time:15:04:05.000} %{shortfunc}: " +
			"%{level:.4s} %{id:03x}%{color:reset} %{message}",
	)
	backend := logging.NewLogBackend(os.Stderr, "[exasol]", 0)
	formattedBackend := logging.NewBackendFormatter(backend, logFormat)
	leveledBackend := logging.AddModuleLevel(formattedBackend)
	leveledBackend.SetLevel(logLevel, "exasol")
	c.log.SetBackend(leveledBackend)
}

func (c *Conn) login() {
	c.log.Notice("EXA: Logging in")
	loginReq := &loginJSON{
		Command:         "login",
		ProtocolVersion: 1,
	}
	res, err := c.send(loginReq) // TODO change req to pointer
	if err != nil {
		c.log.Fatal("Unable to login to EXASOL: ", err)
	}

	pubKeyMod, _ := hex.DecodeString(res["publicKeyModulus"].(string))
	var modulus big.Int
	modulus.SetBytes(pubKeyMod)

	pubKeyExp, _ := strconv.ParseUint(res["publicKeyExponent"].(string), 16, 32)

	pubKey := rsa.PublicKey{
		N: &modulus,
		E: int(pubKeyExp),
	}
	password := []byte(c.Conf.Password)
	encPass, err := rsa.EncryptPKCS1v15(rand.Reader, &pubKey, password)
	if err != nil {
		c.log.Fatal("Pass Enc error:", err, ": ", pubKeyExp)
	}
	b64Pass := base64.StdEncoding.EncodeToString(encPass)

	osUser, _ := user.Current()

	c.log.Notice("EXA: Authenticating")
	authReq := &authJSON{
		Username:         c.Conf.Username,
		Password:         b64Pass,
		UseCompression:   false, // TODO: See if we can get compression working
		ClientName:       c.Conf.ClientName,
		DriverName:       "go-exasol",
		ClientOs:         runtime.GOOS,
		ClientOsUsername: osUser.Username,
		Attributes:       attrJSON{AutoCommit: true}, // Default AutoCommit to on
	}
	resp, err := c.send(authReq)
	if err != nil {
		c.log.Fatal("Unable authenticate with EXASOL: ", err)
	}
	c.SessionID = uint64(resp["sessionId"].(float64))
	c.log.Notice("EXA: Connected SessionID:", c.SessionID)
	c.ws.EnableWriteCompression(false)
}
