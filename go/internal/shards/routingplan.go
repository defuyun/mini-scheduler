package shards

type RoutingPlan struct {
	RoutingTable map[uint64]string `json:"routing_table"`
}

func NewRoutingPlan() *RoutingPlan {
	return &RoutingPlan{
		RoutingTable: make(map[uint64]string),
	}
}
