package shards

import (
	"crypto/rand"
	"errors"
	"time"

	"github.com/oklog/ulid/v2"
)

const MAX_SHARD_DEPTH = 64

type Shard struct {
	ShardID    ulid.ULID `json:"shard_id"`
	Prefix     uint64    `json:"prefix"`
	LocalDepth int       `json:"local_depth"`
}

func (s *Shard) Covers(hashValue uint64) bool {
	shift := 64 - s.LocalDepth
	return (hashValue^s.Prefix)>>shift == 0
}

func NewShard(prefix uint64, localDepth int) *Shard {
	ulid, err := ulid.New(ulid.Timestamp(time.Now()), rand.Reader)
	if err != nil {
		panic(err)
	}

	return &Shard{
		ShardID:    ulid,
		Prefix:     prefix,
		LocalDepth: localDepth,
	}
}

func (s *Shard) Split() (*Shard, *Shard, error) {
	if s.LocalDepth >= MAX_SHARD_DEPTH {
		return nil, nil, errors.New("shard depth is at maximum depth")
	}

	newDepth := s.LocalDepth + 1
	siblingBit := 1 << (64 - newDepth)

	return NewShard(s.Prefix, newDepth), NewShard(s.Prefix|uint64(siblingBit), newDepth), nil
}
