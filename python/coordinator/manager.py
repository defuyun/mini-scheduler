from __future__ import annotations

from etcd.manager import EtcdManager, EventType
from common.constants import SMG_KEY_PREFIX, COORDINATOR_LOCK_TTL_S

from worker.models import WorkerInfo
from shards.provider import ShardsProvider
from coordinator.models import CoordinatorInfo

class CoordinatorManager:
    def __init__(self, coordinator_info: CoordinatorInfo, etcd: EtcdManager):
        self.etcd = etcd
        self.coordinator_info = coordinator_info
        self.workers: dict[str, WorkerInfo] = {}
        self.coordinator_lock_key = f"{SMG_KEY_PREFIX}{self.coordinator_info.service_name}/coordinator/lock"
        self.worker_key_prefix = f"{SMG_KEY_PREFIX}{self.coordinator_info.service_name}/worker"
        self.shard_plan_key_prefix = f"{SMG_KEY_PREFIX}{self.coordinator_info.service_name}/shard/plan"

        self.shard_provider = ShardsProvider(etcd, self.shard_plan_key_prefix)

    def acquire_leadership(self, blocking: bool = True) -> bool:
        self._lock = self.etcd.lock(self.coordinator_lock_key, ttl=COORDINATOR_LOCK_TTL_S)
        return self._lock.acquire(timeout=None if blocking else 0)

    def release_leadership(self) -> None:
        if self._lock and self._lock.is_acquired():
            self._lock.release()

        self._lock = None

    def is_leader(self) -> bool:
        return self._lock is not None and self._lock.is_acquired()

    def _on_worker_change(self, key: bytes, value: bytes) -> None:
        if not value:
            worker_id = key.decode().rsplit("/", 1)[-1]
            self.workers.pop(worker_id, None)
            return

        worker_info = WorkerInfo.model_validate_json(value)
        self.workers[worker_info.worker_id] = worker_info

    def register_worker_watch(self):
        """Load the current set of workers, then track joins and deaths."""
        self.etcd.snapshot_and_subscribe(
            self.worker_key_prefix,
            EventType.ANY,
            on_initial=self._on_worker_change,
            on_event=self._on_worker_change,
        )
    
    def get_workers(self) -> list[WorkerInfo]:
        return self.workers.values()
    
    def start(self):
        if not self.acquire_leadership():
            raise Exception("Failed to acquire leadership, another coordinator is already running")

        self.shard_provider.load_shard_plan()
        self.register_worker_watch()
    
    def shutdown(self):
        self.release_leadership()
