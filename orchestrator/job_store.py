import asyncio
from datetime import datetime
from typing import Dict, Optional, Literal, List, AsyncGenerator
from pydantic import BaseModel, Field
from voice.models import AppSpec

class JobState(BaseModel):
    job_id: str
    spec: AppSpec
    status: Literal["queued", "running", "complete", "failed"] = "queued"
    
    # Agent tracking
    pwa_replica_id: Optional[str] = None
    backend_replica_id: Optional[str] = None
    pwa_status: str = "pending"   # pending | running | complete | failed
    backend_status: str = "pending"
    pwa_logs: List[str] = Field(default_factory=list)
    backend_logs: List[str] = Field(default_factory=list)
    
    # Outputs
    pwa_url: Optional[str] = None
    backend_api_url: Optional[str] = None
    qr_code_data_url: Optional[str] = None  # base64 png
    
    # Timing
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    error: Optional[str] = None

class JobStore:
    def __init__(self):
        self._jobs: Dict[str, JobState] = {}
        self._queues: Dict[str, List[asyncio.Queue]] = {}
    
    def create(self, spec: AppSpec) -> JobState:
        state = JobState(
            job_id=spec.job_id,
            spec=spec,
            status="queued"
        )
        self._jobs[spec.job_id] = state
        self._queues[spec.job_id] = []
        return state
    
    def get(self, job_id: str) -> Optional[JobState]:
        return self._jobs.get(job_id)
    
    async def update(self, job_id: str, **fields) -> JobState:
        state = self._jobs.get(job_id)
        if not state:
            raise KeyError(f"Job {job_id} not found in store")
        
        # Merge dictionary values or replace
        for k, v in fields.items():
            if k in ("pwa_logs", "backend_logs") and isinstance(v, list):
                # Append log entries instead of overwriting entirely
                current_list = getattr(state, k)
                setattr(state, k, current_list + v)
            else:
                setattr(state, k, v)
        
        state.updated_at = datetime.utcnow()
        self._jobs[job_id] = state
        
        # Notify subscribers
        queues = self._queues.get(job_id, [])
        dead_queues = []
        for q in queues:
            try:
                q.put_nowait(state.model_copy())
            except asyncio.QueueFull:
                # Remove queue if full / stale
                dead_queues.append(q)
                
        for dq in dead_queues:
            queues.remove(dq)
            
        return state
        
    async def subscribe(self, job_id: str) -> AsyncGenerator[JobState, None]:
        if job_id not in self._jobs:
            # Yield initial nonexistent state and close
            return
            
        queue = asyncio.Queue(maxsize=100)
        self._queues[job_id].append(queue)
        
        # Yield current initial state
        yield self._jobs[job_id].model_copy()
        
        try:
            while True:
                state = await queue.get()
                yield state
                queue.task_done()
        except asyncio.CancelledError:
            # Unsubscribe on connection drop
            if job_id in self._queues:
                try:
                    self._queues[job_id].remove(queue)
                except ValueError:
                    pass
            raise

# Singleton instance
store = JobStore()
