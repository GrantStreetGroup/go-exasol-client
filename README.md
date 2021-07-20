# go-exasol-client

A Go library for connecting to Exasol based on the [Exasol websocket API](https://github.com/exasol/websocket-api).

Also consider using the [Official Exasol Golang driver](https://github.com/exasol/exasol-driver-go). As of July 2021 the primary differences were:

| Feature | This Driver | Official Driver |
| :- | :-: | :-: |
| Standard Golang SQL driver interface | No | Yes |
| Compression | No | Yes |
| Bulk/Streaming up/download of CSV data | Yes | No |
| Support for alternate/custom websocket libraries | Yes | No |

## Examples

```go
import "github.com/grantstreetgroup/go-exasol-client"

func main() {
    conf := exasol.ConnConf{
        Host:     "host or ip-range",
        Port:     8563,
        Username: "user",
        Password: "pass",
        TLSConfig: &tls.Config{...}, // If specified encryption is enabled
    }
    conn, err = exasol.Connect(conf)
    defer conn.Disconnect()

    conn.DisableAutoCommit()

    conn.Execute("ALTER SESSION SET...")

    // To specify placeholder values you can pass in a second argument that is either
    // []interface{} or [][]interface{} depending on whether you are inserting one or many rows.
    rowsAffected, err := conn.Execute("INSERT INTO t VALUES(?,?,?)", [][]interface{}{...})

    res, err := conn.FetchSlice("SELECT * FROM t WHERE c = ?", []interface{}{...})
    for _, row := range res {
        col = row[0].(string)
    }

    // For large datasets use FetchChan to avoid buffering
    // the entire resultset in memory
    res, err = conn.FetchChan("SELECT * FROM t")
    for row := range res {
        col = row[0].(string)
    }


    // For very large datasets you can send/receive your data
    // in CSV format (stored in a bytes.Buffer) using the Bulk* methods.
    // This is the fastest way to upload or download data to Exasol.
    var csvData new(bytes.Buffer)
    csvData.WriteString("csv,data,...\n...")

    // To upload to a particular table
    err = conn.BulkInsert(schemaName, tableName, csvData)

    // To select all data from a particular table
    err = conn.BulkSelect(schemaName, tableName, csvData)
    SomeCSVParser(csvData.String())

    // To select an arbitrary query
    sql := "EXPORT (SELECT c FROM t) INTO CSV AT '%%s' FILE 'data.csv'"
    err = conn.BulkQuery(sql, csvData)
    SomeCSVParser(csvData.String())


    // For extremely large datasets that cannot fit in memory
    // You can stream your CSV data to/from any of the above Bulk methods
    // by using the equivalent Stream... method.
    // The stream consists of a chan of byte slices where each byte
    // slice is optimally around 8K in size
    csvChan := make(chan []byte, 1000) // Chan size depends on your memory
    go func {
        for {
            ...
            // Generate your CSV data in ~8K chunks
            csvChan <- []byte("csv,data...\n...")
        }
        close(csvChan)
    }
    err := conn.StreamInsert(schemaName, tableName, csvChan)


    res := conn.StreamSelect(schemaName, tableName) // Returns immediately
    // Read your CSV data in ~8K chunks
    for chunk := range res.Data {
        // chunk is a []byte with partial CSV data
    }


    conn.Commit()
}

```

# Author

Grant Street Group <developers@grantstreet.com>

# Copyright and License

This software is Copyright (c) 2019 by Grant Street Group.

This is free software, licensed under:

    MIT License

# Contributors

- Peter Kioko <peter.kioko@grantstreet.com>
