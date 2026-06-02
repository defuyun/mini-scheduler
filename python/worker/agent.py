from __future__ import annotations

from etcd.manager import EtcdManager, EventType
from shards.models import Shard
from worker.models import WorkerInfo, WorkerShardAssignments, WorkerState
from common.constants import SMG_KEY_PREFIX

LEASE_TTL_S = 15


class WorkerAgent:
    def __init__(self, etcd: EtcdManager, worker_info: WorkerInfo):
        self.worker_info = worker_info
        self.etcd = etcd
        self.state = WorkerState.PENDING
        self.worker_key_prefix = f"{SMG_KEY_PREFIX}{self.worker_info.service_name}/worker/{self.worker_info.worker_id}"
        self.watch_id: int | None = None
        self.lease_id = None

        # Immutable snapshot. Writer builds a new tuple and assigns; readers
        # just load the attribute. Single attribute assign/load is atomic at
        # the bytecode level (preserved under free-threaded Python per PEP 703),
        # and tuple immutability keeps in-progress iterations consistent across
        # a swap. No lock needed.
        self._shard_assignments: tuple[Shard, ...] = ()

    def _swap_assignments(self, _key: bytes, value: bytes) -> None:
        if not value:
            self._shard_assignments = ()
            return

        self._shard_assignments = tuple(
            WorkerShardAssignments.model_validate_json(value).shards
        )

    def register_shard_assignments(self):
        self.watch_id = self.etcd.snapshot_and_subscribe(
            self.worker_key_prefix + "/shards",
            EventType.ANY,
            self._swap_assignments,
            self._swap_assignments,
        )
        self.state = WorkerState.REGISTERED

    def join(self):
        self.lease_id = self.etcd.lease(LEASE_TTL_S)
        key = f"{self.worker_key_prefix}"
        value = self.worker_info.model_dump_json().encode("utf-8")
        self.etcd.put(key, value)
        self.state = WorkerState.ACTIVE
        self.register_shard_assignments()

    def get_shard_assignments(self) -> tuple[Shard, ...]:
        return self._shard_assignments

    def shutdown(self):
        if self.watch_id:
            self.etcd.cancel_watch(self.watch_id)
            self.watch_id = None

        self.state = WorkerState.STOPPED
