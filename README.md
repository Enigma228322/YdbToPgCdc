## YDB Async replication app

Create ydb table and changefeed stream. Not neccessary to do it manualy, cause this implemented in code (ydb_service::SetupYdbTableAndCdc)
```sql
CREATE TABLE `/local/my_table` (
    id Uint64,
    data Text,
    PRIMARY KEY (id)
);
ALTER TABLE `/local/my_table`
ADD CHANGEFEED `my_cdc_stream`
WITH (MODE='NEW_AND_OLD_IMAGES', FORMAT='JSON');
```

```bash
go run main.go
```

Insert record
```bash
docker exec -it ydb_server ./ydb -e grpc://ydb:2136 -d /local yql -s "UPSERT INTO my_table (id, data) VALUES (1, 'hello');"
```

Check result
```bash
docker exec -it postgres_server psql -U postgres -d mydb -c "SELECT * FROM my_pg_table ORDER BY id;"
```