package shards

type ShardPlan struct {
	ShardCounts int               `json:"shard_counts"`
	Shards      map[uint64]*Shard `json:"shards"`
}

func NewShardPlan(shardCounts int) *ShardPlan {
	return &ShardPlan{
		ShardCounts: shardCounts,
		Shards:      make(map[uint64]*Shard),
	}
}
