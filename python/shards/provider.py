from __future__ import annotations

from etcd.manager import EtcdManager
from common.constants import SMG_KEY_PREFIX
from shards.models import ShardManagerInfo, ShardPlan


class ShardsProvider:
    def __init__(self, etcd: EtcdManager, shard_plan_key_prefix: str):
        self.etcd = etcd
        self.shard_plan = None
        self.shard_plan_key_prefix = shard_plan_key_prefix

    def load_shard_plan(self):
        value, metadata = self.etcd.get(self.shard_plan_key_prefix)

        if not value:
            self.shard_plan = None
            return

        self.shard_plan = ShardPlan.model_validate_json(value)
    
    def set_shard_plan(self, shard_plan: ShardPlan):
        self.shard_plan = shard_plan
        self.save_shard_plan()
    
    def save_shard_plan(self):
        if self.shard_plan is None:
            self.etcd.delete(self.shard_plan_key_prefix)
            return

        self.etcd.put(
            self.shard_plan_key_prefix, 
            self.shard_plan.model_dump_json().encode("utf-8")
        )
    
    def get_shard_plan(self) -> ShardPlan:
        return self.shard_plan