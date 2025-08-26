package main

import (
	"context"
	"log"
	"os"
	"io"

	"github.com/jackc/pgx/v5"
	"github.com/ydb-platform/ydb-go-sdk/v3"

	"ydb-cdc-go/config"
	"ydb-cdc-go/pg_service"
	"ydb-cdc-go/ydb_service"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logFilePath := config.GetEnv("LOG_FILE", "app.log")
	if f, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err == nil {
		log.SetOutput(io.MultiWriter(os.Stdout, f))
	} else {
		log.Printf("WARN: failed to open log file %s: %v", logFilePath, err)
	}

	ydbEndpoint := config.GetEnv("YDB_ENDPOINT", config.DefaultYdbEndpoint)
	ydbDatabase := config.GetEnv("YDB_DATABASE", config.DefaultYdbDatabase)
	pgConnInfo := config.GetEnv("PG_CONN_INFO", config.DefaultPgConnInfo)

	log.Printf("Starting YDB CDC Processor (Go) with YDB Endpoint: %s, YDB Database: %s, PostgreSQL: %s", ydbEndpoint, ydbDatabase, pgConnInfo)

	db, err := ydb.Open(ctx, ydbEndpoint, ydb.WithDatabase(ydbDatabase))
	log.Printf("ydb connected")
	if err != nil {
		log.Fatalf("Failed to connect to YDB: %v", err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	ydb_service.SetupYdbTableAndCdc(ctx, db, ydbDatabase)

	pgConn, err := pgx.Connect(ctx, pgConnInfo)
	log.Printf("pg connected")
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer func() {
		_ = pgConn.Close(ctx)
	}()
	pg_service.SetupPgTable(ctx, pgConn)

	ydb_service.ReadCdcStream(ctx, db, pgConn)
}
