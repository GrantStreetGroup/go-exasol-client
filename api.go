/*
	AUTHOR

	Grant Street Group <developers@grantstreet.com>

	COPYRIGHT AND LICENSE

	This software is Copyright (c) 2019 by Grant Street Group.
	This is free software, licensed under:
	    MIT License
*/

package exasol

// This is the Version 1.0 API definition based on
// https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV1.md
//
// The struct keys need to be init-caps so that the JSON un/marshaller
// sees them in the websocket library. However Exasol expects them
// to be init lowercase so we need to specify name tag for every
// single one

// Result types
const rowCountType = "rowCount"
const resultSetType = "resultSet"

type request struct {
	Command    string      `json:"command"`
	Attributes *Attributes `json:"attributes,omitempty"`
}

type response struct {
	Status     string      `json:"status"`
	Attributes *Attributes `json:"attributes"`
	Exception  *exception  `json:"exception"`
}

type exception struct {
	Text    string `json:"text"`
	Sqlcode string `json:"sqlcode"`
}

// This struct needs to be visible outside this package
// because it is returned by GetSessionAttr
type Attributes struct {
	Autocommit                  bool   `json:"autocommit,omitempty"`
	CompressionEnabled          bool   `json:"compressionEnabled,omitempty"`
	CurrentSchema               string `json:"currentSchema,omitempty"`
	DateFormat                  string `json:"dateFormat,omitempty"`
	DateLanguage                string `json:"dateLanguage,omitempty"`
	DatetimeFormat              string `json:"datetimeFormat,omitempty"`
	DefaultLikeEscapeCharacter  string `json:"defaultLikeEscapeCharacter,omitempty"`
	FeedbackInterval            uint32 `json:"feedbackInterval,omitempty"`
	NumericCharacters           string `json:"numericCharacters,omitempty"`
	OpenTransaction             int    `json:"openTransaction,omitempty"` // Boolean, really (1/0)
	QueryTimeout                uint32 `json:"queryTimeout,omitempty"`
	SnapshotTransactionsEnabled bool   `json:"snapshotTransactionsEnabled,omitempty"`
	TimestampUtcEnabled         bool   `json:"timestampUtcEnabled,omitempty"`
	Timezone                    string `json:"timezone,omitempty"`
	TimeZoneBehavior            string `json:"timeZoneBehavior,omitempty"`
}

type loginReq struct {
	Command         string      `json:"command"`
	Attributes      *Attributes `json:"attributes,omitempty"`
	ProtocolVersion uint16      `json:"protocolVersion"`
}

type loginRes struct {
	response
	ResponseData *loginData `json:"responseData"`
}

type loginData struct {
	PublicKeyPem      string `json:"publicKeyPem"`
	PublicKeyModulus  string `json:"publicKeyModulus"`
	PublicKeyExponent string `json:"publicKeyExponent"`
}

type authReq struct {
	Username         string      `json:"username"`
	Password         string      `json:"password"`
	UseCompression   bool        `json:"useCompression"`
	ClientName       string      `json:"clientName,omitempty"`
	DriverName       string      `json:"driverName,omitempty"`
	ClientOsUsername string      `json:"clientOsUsername,omitempty"`
	ClientOs         string      `json:"clientOs,omitempty"`
	SessionId        uint64      `json:"sessionId,omitempty"`
	ClientLanguage   string      `json:"clientLanguage,omitempty"`
	ClientVersion    string      `json:"clientVersion,omitempty"`
	ClientRuntime    string      `json:"clientRuntime,omitempty"`
	Attributes       *Attributes `json:"attributes,omitempty"`
}

type authResp struct {
	response
	ResponseData *AuthData `json:"responseData"`
}

type AuthData struct {
	SessionID             uint64  `json:"sessionId"`
	ProtocolVersion       float64 `json:"protocolVersion"`
	ReleaseVersion        string  `json:"releaseVersion"`
	DatabaseName          string  `json:"databaseName"`
	ProductName           string  `json:"productName"`
	MaxDataMessageSize    uint64  `json:"maxDataMessageSize"`
	MaxIdentifierLength   uint64  `json:"maxIdentifierLength"`
	MaxVarcharLength      uint64  `json:"maxVarcharLength"`
	IdentifierQuoteString string  `json:"identifierQuoteString"`
	TimeZone              string  `json:"timeZone"`
	TimeZoneBehavior      string  `json:"timeZoneBehavior"`
}

type execReq struct {
	Command    string      `json:"command"`
	Attributes *Attributes `json:"attributes,omitempty"`
	SqlText    string      `json:"sqlText"`
}

type execPrepStmt struct {
	Command         string          `json:"command"`
	Attributes      *Attributes     `json:"attributes,omitempty"`
	StatementHandle int             `json:"statementHandle"`
	NumColumns      int             `json:"numColumns"`
	NumRows         int             `json:"numRows"`
	Columns         []column        `json:"columns"`
	Data            [][]interface{} `json:"data"`
}

type execRes struct {
	response
	ResponseData *execData `json:"responseData"`
}

type execData struct {
	NumResults uint64   `json:"numResults"`
	Results    []result `json:"results"`
}

type result struct {
	ResultType string     `json:"resultType"`
	RowCount   int64      `json:"rowCount"`
	ResultSet  *resultSet `json:"resultSet"`
}

type resultSet struct {
	ResultSetHandle  int             `json:"resultSetHandle"`
	NumColumns       int             `json:"numColumns"`
	NumRows          uint64          `json:"numRows"`
	NumRowsInMessage int             `json:"numRowsInMessage"`
	Columns          []column        `json:"columns"`
	Data             [][]interface{} `json:"data"`
}

type column struct {
	Name     string   `json:"name"`
	DataType DataType `json:"dataType"`
}

// This is visible outside of this package because
// it is passed in as a connection parameter
type DataType struct {
	Type              string `json:"type"`
	Precision         int    `json:"precision"`
	Scale             int    `json:"scale"`
	Size              int    `json:"size"`
	CharacterSet      string `json:"characterSet,omitempty"`
	WithLocalTimeZone bool   `json:"withLocalTimeZone,omitempty"`
	Fraction          int    `json:"fraction,omitempty"`
	SRId              int    `json:"srid,omitempty"`
}

type fetchReq struct {
	Command         string      `json:"command"`
	Attributes      *Attributes `json:"attributes,omitempty"`
	ResultSetHandle int         `json:"resultSetHandle"`
	StartPosition   uint64      `json:"startPosition"`
	NumBytes        int         `json:"numBytes"`
}

type fetchRes struct {
	response
	ResponseData *fetchData `json:"responseData"`
}

type fetchData struct {
	NumRows uint64          `json:"numRows"`
	Data    [][]interface{} `json:"data"`
}

type closeResultSet struct {
	Command          string      `json:"command"`
	Attributes       *Attributes `json:"attributes,omitempty"`
	ResultSetHandles []int       `json:"resultSetHandles"`
}

type createPrepStmtReq struct {
	Command    string      `json:"command"`
	Attributes *Attributes `json:"attributes,omitempty"`
	SqlText    string      `json:"sqlText"`
}

type createPrepStmtRes struct {
	response
	ResponseData *createPrepStmtData `json:"responseData"`
}

type createPrepStmtData struct {
	StatementHandle int           `json:"statementHandle"`
	ParameterData   parameterData `json:"parameterData"`
	// The API defines the next two fields but they don't
	// seem to make sense in the context of creating a prepared statement
	//numResults
	//results [...]
}

type parameterData struct {
	NumColumns int      `json:"numColumns"`
	Columns    []column `json:"columns"`
}

type closePrepStmt struct {
	Command         string      `json:"command"`
	Attributes      *Attributes `json:"attributes,omitempty"`
	StatementHandle int         `json:"statementHandle"`
}
