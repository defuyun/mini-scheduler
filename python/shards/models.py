from __future__ import annotations

from pydantic import BaseModel
from ulid import ULID

MAX_SHARD_DEPTH = 64


class Shard(BaseModel):
    shard_id: str
    prefix: int
    local_depth: int

    def covers(self, hash_value: int) -> bool:
        shift = 64 - self.local_depth
        return (hash_value ^ self.prefix) >> shift == 0

    def split(self) -> tuple[Shard, Shard]:
        if self.local_depth >= MAX_SHARD_DEPTH:
            raise ValueError(
                f"Shard depth {self.local_depth} is at maximum depth {MAX_SHARD_DEPTH}"
            )

        new_depth = self.local_depth + 1
        sibling_bit = 1 << (64 - new_depth)

        return (
            Shard(shard_id=str(ULID()), prefix=self.prefix, local_depth=new_depth),
            Shard(shard_id=str(ULID()), prefix=self.prefix | sibling_bit, local_depth=new_depth),
        )


class ShardPlan(BaseModel):
    shard_counts: int
    maximum_shard_depth: int
    maximum_shard_count: int
    current_shard_count: int
    shards: list[Shard]


class ShardManagerInfo(BaseModel):
    service_name: str
