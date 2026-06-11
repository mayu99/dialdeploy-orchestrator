# DialDeploy Orchestrator & Voice Gateway

This repository contains the backend engines that drive the DialDeploy platform: the LangGraph orchestration build pipeline and the Pipecat-powered WebRTC voice gateway.

## Components

### 1. Orchestration Gateway (`orchestrator/`)
Runs a FastAPI service that listens for application blueprints (`AppSpec`). It compiles and triggers a step-by-step **LangGraph** workflow:
- **spawn_backend / spawn_pwa**: Spawns concurrent, autonomous **Replicas** coding agents to configure and deploy the database schema on InsForge and refactor the Next.js PWA template.
- **wait_backend / wait_pwa**: Streams agent logs and blocks until the deployments are complete.
- **inject_api_url**: Injects the active database endpoint into the Vercel project environment variables and triggers the build.
- **generate_qr**: Computes a base64 QR code representing the customized PWA URL.

### 2. Voice Gateway (`voice/`)
Manages real-time WebRTC voice dialogue. It provisions **Daily.co** WebRTC rooms, spawns a **Pipecat** conversational bot, and uses Google's **Gemini** model with function calling to translate user requests into structured schema blueprints.

## Tech Stack

- **Backend Framework**: FastAPI (Python)
- **Async Runner**: Uvicorn
- **Orchestration**: LangGraph
- **Voice Agent Pipeline**: Pipecat AI
- **Audio Transport**: Daily.co
- **AI Models**: Google Gemini (`gemini-2.0-flash-exp`)

## Getting Started

### 1. Requirements
- Python 3.10 to 3.13 (Note: Pipecat's dependencies do not support Python >= 3.14).

### 2. Setup Virtual Environment
Create a virtual environment and install dependencies:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Environment Configuration
Create a `.env` file at the root:
```env
# Google AI
GEMINI_API_KEY=your_gemini_api_key

# Daily.co
DAILY_API_KEY=your_daily_api_key
DAILY_DOMAIN=your_daily_domain

# Replicas
REPLICAS_API_KEY=your_replicas_api_key

# InsForge
INSFORGE_TOKEN=your_insforge_token
INSFORGE_PROJECT_ID=your_insforge_project_id

# Vercel
VERCEL_TOKEN=your_vercel_token
VERCEL_PROJECT_ID=your_vercel_project_id

# GitHub
GITHUB_TOKEN=your_github_token
GITHUB_OWNER=your_github_username

# Host config
HOST=http://localhost:8000
```

### 4. Running the Servers

Start the **Orchestrator Backend** (Port 8000):
```bash
venv/Scripts/python -m uvicorn orchestrator.main:app --host 0.0.0.0 --port 8000 --reload
```

Start the **Voice Gateway** (Port 8001):
```bash
venv/Scripts/python -m uvicorn voice.main:app --host 0.0.0.0 --port 8001 --reload
```
