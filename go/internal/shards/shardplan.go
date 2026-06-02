package shards

type ShardPlan struct {
	ShardCounts int     `json:"shard_counts"`
	Shards      []Shard `json:"shards"`
}
