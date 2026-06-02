package shards

type ShardsConfig struct {
	MaximumShardDepth int  `json:"maximum_shard_depth"`
	MaximumShardCount int  `json:"maximum_shard_count"`
	EnableAutoSplit   bool `json:"enable_auto_split"`
}
