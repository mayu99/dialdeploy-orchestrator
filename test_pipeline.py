"""Quick end-to-end pipeline test — submits a job and polls until complete."""
import asyncio
import httpx
import json
import sys
import time

ORCHESTRATOR = "http://localhost:8000"

PAYLOAD = {
    "app_name": "TaskMaster",
    "description": "A task management app for daily productivity",
    "primary_color_hex": "#4F46E5",
    "entities": [{"name": "Task", "fields": ["title", "priority", "due_date"]}],
    "features": ["Create tasks", "Filter by priority", "Mark complete"],
    "session_id": "test-session",
}

async def main():
    async with httpx.AsyncClient(timeout=60.0) as client:
        # 1. Submit job
        print(">>> Submitting job to orchestrator...")
        res = await client.post(f"{ORCHESTRATOR}/jobs", json=PAYLOAD)
        if res.status_code != 200:
            print(f"FAILED to create job: {res.status_code} {res.text}")
            sys.exit(1)
        data = res.json()
        job_id = data["job_id"]
        print(f">>> Job created: {job_id}")

        # 2. Poll until complete/failed
        start = time.time()
        timeout = 120
        last_status = None
        while time.time() - start < timeout:
            res = await client.get(f"{ORCHESTRATOR}/jobs/{job_id}")
            if res.status_code != 200:
                print(f"Poll error: {res.status_code}")
                await asyncio.sleep(2)
                continue
            state = res.json()
            status = state.get("status")
            pwa = state.get("pwa_status")
            backend = state.get("backend_status")
            
            if status != last_status:
                print(f"    Status: {status} | PWA: {pwa} | Backend: {backend}")
                last_status = status
            
            if status in ("complete", "failed"):
                break
            await asyncio.sleep(2)
        
        # 3. Print final state
        elapsed = round(time.time() - start, 1)
        print(f"\n>>> Pipeline finished in {elapsed}s")
        print(f"    Final Status: {state.get('status')}")
        print(f"    PWA Status: {state.get('pwa_status')}")
        print(f"    Backend Status: {state.get('backend_status')}")
        print(f"    PWA URL: {state.get('pwa_url')}")
        print(f"    Backend URL: {state.get('backend_api_url')}")
        print(f"    QR Code: {'YES' if state.get('qr_code_data_url') else 'NO'}")
        print(f"    Error: {state.get('error')}")
        
        if state.get("pwa_logs"):
            print(f"\n    PWA Logs ({len(state['pwa_logs'])} lines):")
            for log in state["pwa_logs"][-5:]:
                print(f"      {log}")
        if state.get("backend_logs"):
            print(f"\n    Backend Logs ({len(state['backend_logs'])} lines):")
            for log in state["backend_logs"][-5:]:
                print(f"      {log}")
        
        if state.get("status") == "complete":
            print("\n✅ PIPELINE COMPLETED SUCCESSFULLY!")
            return 0
        else:
            print(f"\n❌ PIPELINE FAILED: {state.get('error')}")
            return 1

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
