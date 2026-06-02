from fastapi import FastAPI, HTTPException
from ulid import ULID
from worker.agent import WorkerAgent
from worker.models import WorkerInfo, WorkerState, WorkerShardAssignments
from etcd.manager import EtcdManager, EtcdManagerConfig
import uvicorn

app = FastAPI()
worker_agent: WorkerAgent | None = None

@app.get("/worker", response_model=WorkerInfo)
def get_worker_info() -> WorkerInfo:
    if worker_agent is None:
        raise HTTPException(status_code=503, detail="Worker not initialized")

    if worker_agent.state not in (WorkerState.ACTIVE, WorkerState.REGISTERED):
        raise HTTPException(
            status_code=503,
            detail=f"Worker not ready (state={worker_agent.state.value})",
        )

    return worker_agent.worker_info

@app.get("/shards", response_model=WorkerShardAssignments)
def get_shard_assignments() -> WorkerShardAssignments:
    if worker_agent is None:
        raise HTTPException(status_code=503, detail="Worker not initialized")

    if worker_agent.state not in (WorkerState.ACTIVE, WorkerState.REGISTERED):
        raise HTTPException(status_code=503, detail=f"Worker not ready (state={worker_agent.state.value})")

    return WorkerShardAssignments(shards=worker_agent.get_shard_assignments())

if __name__ == "__main__":
    etcd_manager = EtcdManager(EtcdManagerConfig(host="localhost", port=2379, service_name="app"))
    worker_agent = WorkerAgent(etcd_manager, WorkerInfo(service_name="app", worker_id=str(ULID())))
    worker_agent.join()

    uvicorn.run(app, host="0.0.0.0", port=8000)
