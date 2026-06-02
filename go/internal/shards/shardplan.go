package shards

type ShardPlan struct {
	ShardCounts int               `json:"shard_counts"`
	Shards      map[uint64]*Shard `json:"shards"`
}
