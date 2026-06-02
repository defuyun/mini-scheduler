from __future__ import annotations

import etcd3
import etcd3.events
import threading
import time
from typing import Callable, TypeVar
from dataclasses import dataclass
from enum import Enum
from etcd3.client import KVMetadata
from etcd3.watch import WatchResponse

T = TypeVar("T")

@dataclass
class EtcdManagerConfig:
    host: str
    port: int
    service_name: str

class EventType(Enum):
    PUT = "put"
    DELETE = "delete"
    ANY = "any"

    def to_etcd3_event_type(self) -> type[etcd3.events.Event]:
        return {
            EventType.PUT: etcd3.events.PutEvent,
            EventType.DELETE: etcd3.events.DeleteEvent,
            EventType.ANY: etcd3.events.Event,
        }[self]

def keepalive_thread(lease: etcd3.Lease, ttl: int):
    while True:
        time.sleep(ttl / 3)
        try:
            lease.refresh()
        except Exception:
            return

def subscribe_handler(
    response: WatchResponse,
    event_type: EventType,
    callback: Callable[[bytes, bytes], None],
) -> None:
    """Dispatch each event in `response` whose type matches `event_type` to
    `callback(key, value)`. For DELETE events, `value` is empty bytes.
    """
    for event in response.events:
        if isinstance(event, event_type.to_etcd3_event_type()):
            callback(event.key, event.value)

class EtcdManager:
    def __init__(self, config: EtcdManagerConfig):
        self.config = config
        self.lease_id = None
        self.client = etcd3.client(host=config.host, port=config.port)
    
    def add_watch_callback(self, key: str, callback: Callable[[bytes], None]):
        self.client.add_watch_callback(key, callback)
    
    def snapshot_and_subscribe(
        self,
        prefix: str,
        event_type: EventType,
        on_initial: Callable[[bytes, bytes], None],
        on_event: Callable[[bytes, bytes], None],
    ) -> int:
        """Read all keys under `prefix` and subscribe to subsequent changes.

        Guarantees no events are missed between snapshot read and watch start:
        the snapshot's revision is captured and the watch is asked to replay
        everything from `revision + 1`.

        Callbacks receive `(key, value)` as bytes. For DELETE events, `value`
        is empty bytes — switch on `if value:` to distinguish put vs delete.
        """
        snapshot = self.client.get_prefix_response(prefix)
        for kv in snapshot.kvs:
            on_initial(kv.key, kv.value)

        return self.client.add_watch_prefix_callback(
            prefix,
            lambda response: subscribe_handler(response, event_type, on_event),
            start_revision=snapshot.header.revision + 1,
        )
    
    def lease(self, ttl: int):
        self.lease_id = self.client.lease(ttl)
        threading.Thread(
            target=keepalive_thread, 
            args=(self.lease_id, ttl), 
            daemon=True, 
            name=f"{self.config.service_name}-{self.lease_id}-keepalive"
        ).start()
        return self.lease_id
    
    def get(self, key: str) -> tuple[bytes, KVMetadata]:
        return self.client.get(key)
    
    def put(self, key: str, value: bytes):
        return self.client.put(key, value, lease=self.lease_id)
    
    def cancel_watch(self, watch_id: int):
        return self.client.cancel_watch(watch_id)
    
    def lock(self, key: str, ttl: int = 15):
        return self.client.lock(key, ttl=ttl)