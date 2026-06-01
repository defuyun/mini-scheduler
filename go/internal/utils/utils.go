package utils

import (
	"crypto/rand"
	"os"
	"time"

	"github.com/oklog/ulid/v2"
)

func getEnv(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func NewULID() string {
	id, err := ulid.New(ulid.Timestamp(time.Now()), rand.Reader)
	if err != nil {
		panic(err)
	}
	return id.String()
}

func GetWorkerEndpoint() string {
	return getEnv("SMG_ENDPOINT", "127.0.0.1:8000")
}

func GetEtcdEndpoint() string {
	return getEnv("SMG_ETCD_ENDPOINT", "127.0.0.1:2379")
}

func GetServiceName() string {
	return getEnv("SMG_SERVICE_NAME", "app")
}
