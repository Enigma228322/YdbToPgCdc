package ydb_service

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"

	"ydb-cdc-go/pg_service"
)

func SetupYdbTableAndCdc(ctx context.Context, db *ydb.Driver, database string) {
	err := db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.CreateTable(ctx, database+"/my_table",
			options.WithColumn("id", types.Optional(types.TypeUint64)),
			options.WithColumn("data", types.Optional(types.TypeText)),
			options.WithPrimaryKeyColumn("id"),
		)
	})

	if err != nil {
		if ydb.IsOperationErrorAlreadyExistsError(err) {
			log.Printf("YDB table 'my_table' already exists. Skipping creation.")
		} else {
			log.Fatalf("Failed to create YDB table 'my_table': %v", err)
		}
	}
	log.Print("YDB table 'my_table' ensured.")

	descTable, err := db.Table().DescribeTable(ctx, database+"/my_table")
	if err != nil {
		log.Fatalf("Failed to describe YDB table: %v", err)
	}

	if descTable.Changefeeds != nil && len(descTable.Changefeeds) > 0 {
		for _, cf := range descTable.Changefeeds {
			if cf.Name == "my_cdc_stream" && cf.State == options.ChangefeedStateEnabled {
				log.Print("YDB CDC stream 'my_cdc_stream' already exists and is enabled. Skipping creation.")
				return
			}
		}
	}

	err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		query := "ALTER TABLE `" + database + "/my_table` ADD CHANGEFEED `my_cdc_stream` WITH (MODE='NEW_AND_OLD_IMAGES', FORMAT='JSON');"
		return s.ExecuteSchemeQuery(ctx, query)
	})
	if err != nil {
		if ydb.IsOperationErrorAlreadyExistsError(err) {
			log.Print("CDC changefeed already exists. Skipping creation.")
		} else {
			log.Fatalf("Failed to enable YDB CDC stream 'my_cdc_stream': %v", err)
		}
	}
	log.Print("YDB CDC stream 'my_cdc_stream' ensured.")
}

func ReadCdcStream(ctx context.Context, db *ydb.Driver, pgConn *pgx.Conn) {
	consumerName := "my_consumer"
	streamPath := db.Name() + "/my_table/my_cdc_stream"

	if err := db.Topic().Alter(ctx, streamPath,
		topicoptions.AlterWithAddConsumers(topictypes.Consumer{Name: consumerName, ReadFrom: time.Unix(0, 0)}),
	); err != nil {
		if !ydb.IsOperationErrorAlreadyExistsError(err) {
			log.Printf("Failed to add consumer '%s' to '%s': %v", consumerName, streamPath, err)
		}
	}

	desc, err := db.Topic().Describe(ctx, streamPath)
	if err != nil {
		log.Printf("Describe topic failed: %v", err)
	} else {
		log.Printf("Topic '%s': partitions=%d, consumers=%d", streamPath, len(desc.Partitions), len(desc.Consumers))
		for _, c := range desc.Consumers {
			if c.Name == consumerName {
				log.Printf("Consumer '%s': readFrom=%v", c.Name, c.ReadFrom)
			}
		}
	}

	reader, err := db.Topic().StartReader(
		consumerName,
		topicoptions.ReadTopic(streamPath),
		topicoptions.WithReaderGetPartitionStartOffset(func(_ context.Context, _ topicoptions.GetPartitionStartOffsetRequest) (topicoptions.GetPartitionStartOffsetResponse, error) {
			var resp topicoptions.GetPartitionStartOffsetResponse
			resp.StartFrom(0)
			return resp, nil
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create YDB Topic Reader: %v", err)
	}
	defer func() { _ = reader.Close(ctx) }()

	log.Printf("Starting to read from YDB CDC stream '%s'...", streamPath)

	lastWaitLog := time.Now()
	for {
		if ctx.Err() != nil {
			log.Print("Context cancelled, stopping CDC stream reader.")
			return
		}

		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Print("Context cancelled during read, stopping CDC stream reader.")
				return
			}
			if time.Since(lastWaitLog) > 5*time.Second {
				log.Printf("Waiting for CDC messages from '%s'...", streamPath)
				lastWaitLog = time.Now()
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}

		payload, err := io.ReadAll(msg)
		if err != nil {
			log.Printf("Failed to read message payload: %v", err)
			continue
		}

		log.Printf("Received CDC message from partition %d, offset %d: %s", msg.PartitionID(), msg.Offset, string(payload))

		var cdcEvent map[string]interface{}
		if err := json.Unmarshal(payload, &cdcEvent); err != nil {
			log.Printf("Failed to parse JSON CDC message: %v", err)
			continue
		}

		id, ok := cdcEvent["id"].(float64)
		if !ok {
			log.Printf("Missing or invalid 'id' in CDC message: %+v", cdcEvent)
			continue
		}
		data, ok := cdcEvent["data"].(string)
		if !ok {
			log.Printf("Missing or invalid 'data' in CDC message: %+v", cdcEvent)
			continue
		}

		if err := pg_service.InsertIntoPg(ctx, pgConn, uint64(id), data); err != nil {
			log.Printf("Failed to insert/update into PostgreSQL: %v", err)
		}

		_ = reader.Commit(ctx, msg)
	}
}
