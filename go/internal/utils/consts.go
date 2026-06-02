package utils

import "fmt"

const SMG_KEY_PREFIX = "/mini-scheduler"

func GetServiceKey(serviceName string) string {
	return fmt.Sprintf("%s/%s", SMG_KEY_PREFIX, serviceName)
}

func GetShardManagerKey(serviceName string) string {
	return fmt.Sprintf("%s/%s/%s", SMG_KEY_PREFIX, serviceName, "shard-manager")
}

func GetWorkerKey(serviceName string, workerID string) string {
	return fmt.Sprintf("%s/%s/%s/%s", SMG_KEY_PREFIX, serviceName, "worker", workerID)
}

func GetWorkerShardsKey(serviceName string, workerID string) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", SMG_KEY_PREFIX, serviceName, "worker", workerID, "shards")
}

func GetShardPlanKey(serviceName string) string {
	return fmt.Sprintf("%s/%s/%s", SMG_KEY_PREFIX, serviceName, "shardplan")
}

func GetRoutingPlanKey(serviceName string) string {
	return fmt.Sprintf("%s/%s/%s", SMG_KEY_PREFIX, serviceName, "routingplan")
}
