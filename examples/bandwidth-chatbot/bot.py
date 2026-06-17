#
# Copyright (c) 2026, Bandwidth Inc.
#
# SPDX-License-Identifier: BSD-2-Clause
#

"""Foundational Bandwidth + Pipecat example.

A single-file FastAPI application that handles a Bandwidth Programmable Voice
inbound call:

1. ``POST /`` (Basic Auth) is the Bandwidth voice webhook. We pull the
   server-trusted ``callId``/``accountId`` straight from the (authenticated)
   webhook body, mint a one-time correlation token, and return a
   ``<StartStream>`` BXML pointing at ``wss://<host>/ws/<token>``.
2. ``/ws/{token}`` validates the token, looks up the trusted call/account
   IDs from server-side state, and runs a Pipecat pipeline:

       Bandwidth WS  ->  Deepgram STT  ->  OpenAI LLM  ->  Cartesia TTS  ->  Bandwidth WS

This trust chain matters: without it, anyone who reaches the WebSocket can
hand us an arbitrary callId in the ``start`` event metadata and the
serializer's auto-hang-up path will fire the operator's OAuth credentials at
that callId, terminating live calls.

Run::

    uv sync
    cp env.example .env  # fill in API keys + Bandwidth credentials
    uv run python bot.py

Then point an ngrok tunnel at port 7860 and configure your Bandwidth
application's voice webhook to ``https://<your-ngrok-host>/`` with HTTP Basic
Auth using the same username/password you set in ``.env``.
"""

import asyncio
import base64
import json
import os
import secrets
import time

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, WebSocket, status
from fastapi.responses import HTMLResponse
from loguru import logger
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import LLMRunFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)
from pipecat.workers.runner import WorkerRunner

from pipecat_bandwidth import BandwidthFrameSerializer

load_dotenv(override=True)

PROXY_HOST = os.getenv("PROXY_HOST", "")  # e.g. "abcd-12-34-56-78.ngrok-free.app"
PORT = int(os.getenv("PORT", "7860"))

# Webhook auth. Configure the same credentials in your Bandwidth voice
# application's webhook settings. Without this, anyone who reaches POST / can
# forge a webhook body and obtain a /ws/{token} URL bound to an arbitrary
# callId.
WEBHOOK_USERNAME = os.getenv("BANDWIDTH_WEBHOOK_USERNAME", "")
WEBHOOK_PASSWORD = os.getenv("BANDWIDTH_WEBHOOK_PASSWORD", "")

# How long an issued token is valid before Bandwidth must connect to /ws.
TOKEN_TTL_SECONDS = 60

app = FastAPI()


# In-memory token → (call_id, account_id, expires_at) store. Single-process
# only; for multi-worker deployments back this with Redis or similar.
_pending_calls: dict[str, tuple[str, str, float]] = {}
_pending_calls_lock = asyncio.Lock()


def _verify_webhook_auth(request: Request) -> None:
    """Reject the request unless Basic Auth matches the configured creds."""
    if not WEBHOOK_USERNAME or not WEBHOOK_PASSWORD:
        # Refuse to run without webhook auth — silent acceptance would
        # reintroduce the very vulnerability this example exists to avoid.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook authentication is not configured on the server.",
        )

    auth_header = request.headers.get("authorization", "")
    if not auth_header.lower().startswith("basic "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Webhook requires Basic Auth",
            headers={"WWW-Authenticate": "Basic"},
        )
    try:
        decoded = base64.b64decode(auth_header.split(" ", 1)[1]).decode("utf-8")
    except (ValueError, UnicodeDecodeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Malformed credentials"
        ) from exc
    username, _, password = decoded.partition(":")

    user_ok = secrets.compare_digest(username, WEBHOOK_USERNAME)
    pass_ok = secrets.compare_digest(password, WEBHOOK_PASSWORD)
    if not (user_ok and pass_ok):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid webhook credentials",
            headers={"WWW-Authenticate": "Basic"},
        )


async def _issue_token(call_id: str, account_id: str) -> str:
    """Mint a one-time token bound to a server-trusted (call_id, account_id)."""
    token = secrets.token_urlsafe(32)
    expires_at = time.monotonic() + TOKEN_TTL_SECONDS
    async with _pending_calls_lock:
        _pending_calls[token] = (call_id, account_id, expires_at)
    return token


async def _consume_token(token: str) -> tuple[str, str] | None:
    """Pop and validate a token, returning the trusted IDs or None."""
    now = time.monotonic()
    async with _pending_calls_lock:
        # Drop anything stale while we hold the lock.
        for stale in [t for t, (_, _, exp) in _pending_calls.items() if exp < now]:
            _pending_calls.pop(stale, None)
        entry = _pending_calls.pop(token, None)
    if entry is None:
        return None
    call_id, account_id, expires_at = entry
    if expires_at < now:
        return None
    return call_id, account_id


@app.post("/")
async def inbound_call(request: Request) -> HTMLResponse:
    """Bandwidth voice webhook. Basic Auth required; trust the body's IDs."""
    _verify_webhook_auth(request)

    body = await request.json()
    call_id = body.get("callId")
    account_id = body.get("accountId")
    if not call_id or not account_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Webhook body missing callId or accountId",
        )

    token = await _issue_token(str(call_id), str(account_id))
    logger.info(f"Issued WS token for call_id={call_id}")

    bxml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <StartStream destination="wss://{PROXY_HOST}/ws/{token}" mode="bidirectional" tracks="inbound"/>
  <Pause duration="86400"/>
</Response>"""
    return HTMLResponse(content=bxml, media_type="application/xml")


@app.websocket("/ws/{token}")
async def websocket_endpoint(websocket: WebSocket, token: str) -> None:
    """Validate the correlation token, then run the bot pipeline."""
    trusted = await _consume_token(token)
    if trusted is None:
        # 1008 = "policy violation" per RFC 6455.
        await websocket.close(code=1008)
        logger.warning("Rejected WS connect with invalid or expired token")
        return

    call_id, account_id = trusted
    await websocket.accept()

    # The first frame from Bandwidth is the "start" event. We only consume
    # streamId from it (it's just a wire-protocol identifier, not an
    # authorization input). callId and accountId come from the trusted token
    # mapping above — we deliberately ignore whatever the WS metadata claims.
    first = await websocket.receive_text()
    start_event = json.loads(first)
    metadata = start_event.get("metadata", {})
    stream_id = metadata.get("streamId", "")

    logger.info(f"Bandwidth stream started: stream_id={stream_id} call_id={call_id}")

    serializer = BandwidthFrameSerializer(
        stream_id=stream_id,
        call_id=call_id,
        account_id=account_id,
        client_id=os.getenv("BANDWIDTH_CLIENT_ID"),
        client_secret=os.getenv("BANDWIDTH_CLIENT_SECRET"),
    )

    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            add_wav_header=False,
            serializer=serializer,
        ),
    )

    await run_bot(transport)


async def run_bot(transport: FastAPIWebsocketTransport) -> None:
    """Build and run the Pipecat pipeline for one call."""
    stt = DeepgramSTTService(api_key=os.getenv("DEEPGRAM_API_KEY"))

    llm = OpenAILLMService(
        api_key=os.getenv("OPENAI_API_KEY"),
        settings=OpenAILLMService.Settings(
            system_instruction=(
                "You are a helpful assistant on a phone call. Keep responses "
                "concise and conversational. Avoid special characters since "
                "your output is converted to audio."
            ),
        ),
    )

    tts = CartesiaTTSService(
        api_key=os.getenv("CARTESIA_API_KEY"),
        settings=CartesiaTTSService.Settings(
            voice="71a7ad14-091c-4e8e-a314-022ece01c121",  # British Reading Lady
        ),
    )

    context = LLMContext()
    user_aggregator, assistant_aggregator = LLMContextAggregatorPair(
        context,
        user_params=LLMUserAggregatorParams(vad_analyzer=SileroVADAnalyzer()),
    )

    pipeline = Pipeline(
        [
            transport.input(),
            stt,
            user_aggregator,
            llm,
            tts,
            transport.output(),
            assistant_aggregator,
        ]
    )

    worker = PipelineWorker(
        pipeline,
        params=PipelineParams(
            audio_in_sample_rate=8000,
            audio_out_sample_rate=8000,
            enable_metrics=True,
            enable_usage_metrics=True,
        ),
    )

    @transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        context.add_message({"role": "user", "content": "Please introduce yourself to the caller."})
        await worker.queue_frames([LLMRunFrame()])

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        await worker.cancel()

    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(worker)
    await runner.run()


if __name__ == "__main__":
    import uvicorn

    if not PROXY_HOST:
        logger.warning(
            "PROXY_HOST is not set — the BXML response will reference an empty "
            "host. Set PROXY_HOST in .env to your public ngrok hostname."
        )
    if not WEBHOOK_USERNAME or not WEBHOOK_PASSWORD:
        logger.warning(
            "BANDWIDTH_WEBHOOK_USERNAME / BANDWIDTH_WEBHOOK_PASSWORD are not "
            "set; POST / will return 503 until you configure them."
        )

    uvicorn.run(app, host="0.0.0.0", port=PORT)
