import os
import asyncio
import uuid
from typing import Optional
from fastapi import FastAPI, HTTPException, APIRouter, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import httpx
from dotenv import load_dotenv

# Import our session manager and Pydantic models
from voice.session_manager import create_room, create_meeting_token, delete_room, session_store
from voice.models import AppSpec, Entity

load_dotenv()

# Setup router
router = APIRouter()

# Pipecat imports
try:
    from pipecat.pipeline.pipeline import Pipeline
    from pipecat.pipeline.task import PipelineTask
    from pipecat.pipeline.runner import PipelineRunner
    from pipecat.transports.services.daily import DailyTransport, DailyParams
    from pipecat.services.google import GoogleLLMService, GoogleTTSService
    from pipecat.processors.aggregators.llm_response import (
        LLMAssistantResponseAggregator,
        LLMUserResponseAggregator,
    )
    PIPECAT_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Pipecat dependencies could not be imported ({e}). Running in Mock mode.")
    PIPECAT_AVAILABLE = False


class CreateSessionResponse(BaseModel):
    room_url: str
    user_token: str
    job_id: str
    session_id: str


# System prompt for Gemini voice developer bot
SYSTEM_PROMPT = (
    "You are DialDeploy, a friendly voice assistant that helps users design mobile apps. "
    "Your goal is to extract the details of the app they want to build. "
    "Keep your conversation natural and friendly, and ask short questions one at a time. "
    "Specifically, you must extract: "
    "1. The application name. "
    "2. A one-sentence description of the application. "
    "3. One primary entity (like 'Task', 'Habit', 'Expense') and a couple of fields (like 'title', 'amount'). "
    "4. A list of 2 to 4 core features. "
    "Keep questions short. Maximum 6 questions total. When you have collected this information, "
    "call the extract_app_spec tool to trigger the build, say: "
    "'Got it. Your app is being built — watch the dashboard. This call will end now.' "
    "and then stop talking."
)


# Define extract_app_spec tool for Gemini Function Calling
EXTRACT_APP_SPEC_TOOL = {
    "function_declarations": [
        {
            "name": "extract_app_spec",
            "description": "Trigger the build process by extracting the completed AppSpec",
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "app_name": {"type": "STRING", "description": "The name of the app, e.g. DailyStreak"},
                    "description": {"type": "STRING", "description": "One line description of the app"},
                    "primary_color_hex": {"type": "STRING", "description": "Primary accent hex color, default #4F46E5"},
                    "entities": {
                        "type": "ARRAY",
                        "description": "List of 1 to 3 database entities to track",
                        "items": {
                            "type": "OBJECT",
                            "properties": {
                                "name": {"type": "STRING", "description": "Entity singular name, e.g. Habit"},
                                "fields": {
                                    "type": "ARRAY",
                                    "description": "Fields associated with this entity",
                                    "items": {"type": "STRING"}
                                }
                            },
                            "required": ["name", "fields"]
                        }
                    },
                    "features": {
                        "type": "ARRAY",
                        "description": "List of 2 to 4 key features",
                        "items": {"type": "STRING"}
                    }
                },
                "required": ["app_name", "description", "entities", "features"]
            }
        }
    ]
}


async def run_pipecat_bot(room_url: str, token: str, session_id: str, job_id: str):
    """Runs the Pipecat WebRTC Bot in the Daily.co Room"""
    if not PIPECAT_AVAILABLE:
        print(f"Mock Bot: Joining room {room_url} with token {token[:10]}...")
        # Simulate voice conversation wait, then issue mock callback
        await asyncio.sleep(15)
        await trigger_spec_callback(session_id, job_id, AppSpec(
            app_name="DailyStreak",
            description="A modern habit tracker to log and visualize daily habits",
            entities=[Entity(name="Habit", fields=["title", "frequency", "streak_count"])],
            features=["Log habit progress", "Reset streaks", "Browse all logs"],
            primary_color_hex="#4F46E5",
            session_id=session_id,
            job_id=job_id
        ))
        return

    try:
        # 1. Initialize Daily WebRTC transport
        transport = DailyTransport(
            room_url=room_url,
            token=token,
            bot_name="DialDeploy Bot",
            params=DailyParams(
                audio_in_enabled=True,
                audio_out_enabled=True,
                vad_enabled=True,
                transcription_enabled=True  # Use Daily's built-in transcription for STT
            )
        )

        # 2. Setup Services using Gemini API Key
        api_key = os.getenv("GEMINI_API_KEY", "")
        llm = GoogleLLMService(
            api_key=api_key,
            model="gemini-2.0-flash-exp",
            system_instruction=SYSTEM_PROMPT,
            tools=EXTRACT_APP_SPEC_TOOL
        )
        tts = GoogleTTSService(
            api_key=api_key,
            voice_id="en-US-Neural2-F"  # Standard neural voice
        )

        # 3. Setup Response Aggregators for conversational turns
        user_aggregator = LLMUserResponseAggregator()
        assistant_aggregator = LLMAssistantResponseAggregator()

        # 4. Wire the pipeline flow
        # Inward: User Audio -> STT Transcription -> LLM Aggregator -> Gemini
        # Outward: Gemini Responses -> Assistant Aggregator -> TTS Audio -> Daily Audio Out
        pipeline = Pipeline([
            transport.input(),
            user_aggregator,
            llm,
            assistant_aggregator,
            tts,
            transport.output()
        ])

        task = PipelineTask(pipeline)

        # Define tool callback function
        @llm.register_action("extract_app_spec")
        async def handle_extract_app_spec(function_name, tool_call_id, arguments, llm, pipeline, **kwargs):
            try:
                print(f"Voice LLM called tool extract_app_spec: {arguments}")
                spec = AppSpec(
                    app_name=arguments.get("app_name", "DialApp"),
                    description=arguments.get("description", "A custom DialDeploy App"),
                    entities=[
                        Entity(name=e.get("name"), fields=e.get("fields"))
                        for e in arguments.get("entities", [])
                    ],
                    features=arguments.get("features", []),
                    primary_color_hex=arguments.get("primary_color_hex", "#4F46E5"),
                    session_id=session_id,
                    job_id=job_id
                )
                await trigger_spec_callback(session_id, job_id, spec)
                
                # Signal conversational completion text back to user
                await task.queue_text_speech("Got it. Your app is being built — watch the dashboard. This call will end now.")
                await asyncio.sleep(4.0)
                await task.stop()
            except Exception as e:
                print(f"Error executing tool extract_app_spec: {e}")
                await task.queue_text_speech("I encountered an error starting your build. Please try again.")

        runner = PipelineRunner()
        await runner.run(task)

    except Exception as e:
        print(f"Error in Pipecat Bot runtime execution: {e}")
    finally:
        # Cleanup Daily Room
        try:
            await delete_room(session_id)
        except Exception as ex:
            print(f"Failed to delete room during cleanup: {ex}")


async def trigger_spec_callback(session_id: str, job_id: str, spec: AppSpec):
    """Triggers the orchestrator API webhook endpoint with the structured AppSpec"""
    orchestrator_url = os.getenv("HOST", "http://localhost:8000")
    print(f"POSTing AppSpec to Orchestrator at {orchestrator_url}/jobs")
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{orchestrator_url}/jobs",
                json=spec.model_dump(),
                headers={"Content-Type": "application/json"},
                timeout=10.0
            )
            print(f"Orchestrator webhook response: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"Failed to post AppSpec to orchestrator callback: {e}")


@router.post("/voice/create-session", response_model=CreateSessionResponse)
async def create_session(background_tasks: BackgroundTasks):
    try:
        # Create Daily room
        room = await create_room()
        room_name = room.get("name", "")
        room_url = room.get("url", "")
        
        # Generate meeting tokens
        user_token = await create_meeting_token(room_name, is_bot=False)
        bot_token = await create_meeting_token(room_name, is_bot=True)
        
        job_id = str(uuid.uuid4())
        session_store[room_name] = (user_token, bot_token, job_id)
        
        # Spawn Pipecat bot in background
        background_tasks.add_task(
            run_pipecat_bot,
            room_url,
            bot_token,
            room_name,
            job_id
        )
        
        return CreateSessionResponse(
            room_url=room_url,
            user_token=user_token,
            job_id=job_id,
            session_id=room_name
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/voice/sessions/{session_id}/status")
async def get_session_status(session_id: str):
    if session_id in session_store:
        return {"active": True, "job_id": session_store[session_id][2]}
    return {"active": False}


# Setup FastAPI standalone app for voice if needed
app = FastAPI(title="DialDeploy Voice Agent Gateway")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("voice.main:app", host="0.0.0.0", port=8001, reload=True)
