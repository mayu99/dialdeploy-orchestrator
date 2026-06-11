import asyncio
import base64
from io import BytesIO
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from sse_starlette.sse import EventSourceResponse

# Import models, store, graph
from voice.models import AppSpec
from orchestrator.job_store import store, JobState
from orchestrator.graph import orchestrator_graph

app = FastAPI(title="DialDeploy Orchestration Gateway")

# Setup CORS for local dashboard and Vercel hosting
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

JOBS_THIS_SESSION = 0
MAX_JOBS = 10

async def run_orchestrator_pipeline(spec: AppSpec):
    """Triggers the compiled LangGraph workflow concurrently"""
    try:
        print(f"Starting orchestration pipeline for job {spec.job_id}...")
        initial_state = {"job_id": spec.job_id, "spec": spec}
        await orchestrator_graph.ainvoke(initial_state)
        print(f"Orchestration pipeline finished for job {spec.job_id}.")
    except Exception as e:
        print(f"Exception during orchestration pipeline execution: {e}")
        await store.update(spec.job_id, status="failed", error=str(e))

@app.post("/jobs")
async def create_job(spec: AppSpec, background_tasks: BackgroundTasks):
    global JOBS_THIS_SESSION
    JOBS_THIS_SESSION += 1
    if JOBS_THIS_SESSION > MAX_JOBS:
        raise HTTPException(status_code=429, detail="Session job limits reached. Restart the server.")

    # Create job in memory store
    store.create(spec)
    
    # Spawn graph execution in background task (immediate 200ms response return)
    background_tasks.add_task(run_orchestrator_pipeline, spec)
    
    return {"job_id": spec.job_id, "status": "queued"}

@app.get("/jobs/{job_id}", response_model=JobState)
async def get_job(job_id: str):
    job = store.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.get("/jobs/{job_id}/stream")
async def stream_job_updates(job_id: str):
    job = store.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
        
    async def sse_generator():
        async for state in store.subscribe(job_id):
            # Format as JSON string for Server-Sent Events standard
            yield {"data": state.model_dump_json()}
            
    return EventSourceResponse(sse_generator())

@app.get("/jobs/{job_id}/qr")
async def get_job_qr(job_id: str):
    job = store.get(job_id)
    if not job or not job.qr_code_data_url:
        raise HTTPException(status_code=404, detail="QR code not available for this job")
        
    try:
        # Extract base64 payload from data URL scheme
        header, base64_payload = job.qr_code_data_url.split(",", 1)
        qr_bytes = base64.b64decode(base64_payload)
        
        return StreamingResponse(BytesIO(qr_bytes), media_type="image/png")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to decode QR code: {e}")

from uuid import uuid4
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class MockItem(BaseModel):
    id: str
    title: str
    description: Optional[str] = None
    completed: bool = False
    created_at: str

class CreateMockItemInput(BaseModel):
    title: str
    description: Optional[str] = None

class UpdateMockItemInput(BaseModel):
    completed: Optional[bool] = None
    title: Optional[str] = None
    description: Optional[str] = None

from collections import defaultdict
from typing import Dict

# In-memory store for mock items partitioned by job_id
mock_items_db: Dict[str, List[MockItem]] = defaultdict(list)

@app.get("/items", response_model=List[MockItem])
async def list_mock_items():
    return mock_items_db["default"]

@app.post("/items", response_model=MockItem)
async def create_mock_item(item_input: CreateMockItemInput):
    new_item = MockItem(
        id=str(uuid4()),
        title=item_input.title,
        description=item_input.description,
        completed=False,
        created_at=datetime.utcnow().isoformat() + "Z"
    )
    mock_items_db["default"].append(new_item)
    return new_item

@app.get("/items/{item_id}", response_model=MockItem)
async def get_mock_item(item_id: str):
    for item in mock_items_db["default"]:
        if item.id == item_id:
            return item
    raise HTTPException(status_code=404, detail="Item not found")

@app.patch("/items/{item_id}", response_model=MockItem)
async def update_mock_item(item_id: str, item_input: UpdateMockItemInput):
    for item in mock_items_db["default"]:
        if item.id == item_id:
            if item_input.completed is not None:
                item.completed = item_input.completed
            if item_input.title is not None:
                item.title = item_input.title
            if item_input.description is not None:
                item.description = item_input.description
            return item
    raise HTTPException(status_code=404, detail="Item not found")

# Partitioned endpoints
@app.get("/api/mock/{job_id}/items", response_model=List[MockItem])
async def list_partitioned_mock_items(job_id: str):
    return mock_items_db[job_id]

@app.post("/api/mock/{job_id}/items", response_model=MockItem)
async def create_partitioned_mock_item(job_id: str, item_input: CreateMockItemInput):
    new_item = MockItem(
        id=str(uuid4()),
        title=item_input.title,
        description=item_input.description,
        completed=False,
        created_at=datetime.utcnow().isoformat() + "Z"
    )
    mock_items_db[job_id].append(new_item)
    return new_item

@app.get("/api/mock/{job_id}/items/{item_id}", response_model=MockItem)
async def get_partitioned_mock_item(job_id: str, item_id: str):
    for item in mock_items_db[job_id]:
        if item.id == item_id:
            return item
    raise HTTPException(status_code=404, detail="Item not found")

@app.patch("/api/mock/{job_id}/items/{item_id}", response_model=MockItem)
async def update_partitioned_mock_item(job_id: str, item_id: str, item_input: UpdateMockItemInput):
    for item in mock_items_db[job_id]:
        if item.id == item_id:
            if item_input.completed is not None:
                item.completed = item_input.completed
            if item_input.title is not None:
                item.title = item_input.title
            if item_input.description is not None:
                item.description = item_input.description
            return item
    raise HTTPException(status_code=404, detail="Item not found")

@app.get("/health")
async def health_check():
    return {"ok": True, "jobs_allocated": JOBS_THIS_SESSION}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("orchestrator.main:app", host="0.0.0.0", port=8000, reload=True)
