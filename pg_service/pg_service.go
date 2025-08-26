package pg_service

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5"
)

func SetupPgTable(ctx context.Context, pgConn *pgx.Conn) {
	_, err := pgConn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS my_pg_table (
			id BIGINT PRIMARY KEY,
			data TEXT
		);
	`)
	if err != nil {
		log.Fatalf("Failed to create PostgreSQL table: %v", err)
	}
	log.Print("PostgreSQL table 'my_pg_table' ensured.")
}

func InsertIntoPg(ctx context.Context, pgConn *pgx.Conn, id uint64, data string) error {
	_, err := pgConn.Exec(ctx, `
		INSERT INTO my_pg_table (id, data) VALUES ($1, $2)
		ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data;
	`, id, data)
	return err
}
