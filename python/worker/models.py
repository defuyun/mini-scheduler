from __future__ import annotations

from enum import Enum

from pydantic import BaseModel

from shards.models import Shard

class WorkerState(Enum):
    PENDING = "started"
    STOPPED = "stopped"
    ACTIVE = "active"
    REGISTERED = "registered"


class WorkerInfo(BaseModel):
    service_name: str
    worker_id: str

class WorkerShardAssignments(BaseModel):
    shards: list[Shard]