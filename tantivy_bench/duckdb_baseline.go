package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// DuckDBBaseline manages DuckDB for writing Parquet and running LIKE queries.
type DuckDBBaseline struct {
	db          *sql.DB
	parquetPath string
}

// NewDuckDBBaseline creates a DuckDB instance and writes records to Parquet.
func NewDuckDBBaseline(records []LogRecord, tmpDir string) (*DuckDBBaseline, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	b := &DuckDBBaseline{db: db, parquetPath: filepath.Join(tmpDir, "logs.parquet")}

	if err := b.writeParquet(records); err != nil {
		db.Close()
		return nil, err
	}

	return b, nil
}

func (b *DuckDBBaseline) writeParquet(records []LogRecord) error {
	// Create table
	_, err := b.db.Exec(`CREATE TABLE logs (
		timestamp BIGINT,
		level VARCHAR,
		service VARCHAR,
		host VARCHAR,
		method VARCHAR,
		path VARCHAR,
		message VARCHAR,
		doc_id VARCHAR
	)`)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	// Batch insert using prepared statement
	tx, err := b.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	stmt, err := tx.Prepare("INSERT INTO logs VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare: %w", err)
	}

	batchSize := 10000
	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}
		for _, r := range records[i:end] {
			if _, err := stmt.Exec(r.Timestamp, r.Level, r.Service, r.Host, r.Method, r.Path, r.Message, r.DocID); err != nil {
				tx.Rollback()
				return fmt.Errorf("exec insert: %w", err)
			}
		}
	}

	stmt.Close()
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	// Export to Parquet
	if _, err := b.db.Exec(fmt.Sprintf("COPY logs TO '%s' (FORMAT PARQUET, COMPRESSION SNAPPY)", b.parquetPath)); err != nil {
		return fmt.Errorf("copy to parquet: %w", err)
	}

	// Drop in-memory table, re-create as view over Parquet (matches Arc's query pattern)
	if _, err := b.db.Exec("DROP TABLE logs"); err != nil {
		return fmt.Errorf("drop table: %w", err)
	}
	if _, err := b.db.Exec(fmt.Sprintf("CREATE VIEW logs AS SELECT * FROM read_parquet('%s')", b.parquetPath)); err != nil {
		return fmt.Errorf("create view: %w", err)
	}

	return nil
}

// ParquetSize returns the Parquet file size in bytes.
func (b *DuckDBBaseline) ParquetSize() int64 {
	fi, err := os.Stat(b.parquetPath)
	if err != nil {
		return 0
	}
	return fi.Size()
}

// RunQuery executes a SQL query and returns the count result and duration.
func (b *DuckDBBaseline) RunQuery(query string) (count int64, dur time.Duration, err error) {
	start := time.Now()
	err = b.db.QueryRow(query).Scan(&count)
	dur = time.Since(start)
	return
}

// Close releases DuckDB resources.
func (b *DuckDBBaseline) Close() {
	b.db.Close()
}
