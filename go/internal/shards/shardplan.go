package shards

type ShardPlan struct {
	Shards map[uint64]*Shard `json:"shards"`
}

func NewShardPlan() *ShardPlan {
	return &ShardPlan{
		Shards: make(map[uint64]*Shard),
	}
}
