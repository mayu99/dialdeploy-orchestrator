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

@app.get("/health")
async def health_check():
    return {"ok": True, "jobs_allocated": JOBS_THIS_SESSION}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("orchestrator.main:app", host="0.0.0.0", port=8000, reload=True)
