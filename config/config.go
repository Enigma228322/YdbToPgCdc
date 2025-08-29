package config

import "os"

const (
	DefaultYdbEndpoint = "grpc://0.0.0.0:2136"
	DefaultYdbDatabase = "/local"
	DefaultPgConnInfo  = "postgres://postgres:mysecretpassword@localhost:5432/mydb"
)

func GetEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
