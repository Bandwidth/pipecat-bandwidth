#
# Copyright (c) 2026, Bandwidth Inc.
#
# SPDX-License-Identifier: BSD-2-Clause
#

"""Speech-to-speech Bandwidth + Pipecat example using OpenAI Realtime.

Same inbound trust chain as ``bot.py``, but the three-service cascade
(Deepgram STT -> OpenAI LLM -> Cartesia TTS) collapses into a single
speech-to-speech service:

    Bandwidth WS  ->  OpenAI Realtime (STT + LLM + TTS)  ->  Bandwidth WS

This needs only an OpenAI API key for the AI side — no Deepgram, no
Cartesia. ``OpenAIRealtimeLLMService`` ingests caller audio and emits the
reply audio directly, and the OpenAI Realtime API performs server-side turn
detection, so there is no separate VAD analyzer in the pipeline.

Two details specific to the realtime model:

1. It works in 24 kHz PCM both directions. We run the pipeline at 24 kHz and
   configure the serializer for ``PCM`` output at 24 kHz; the serializer
   already resamples Bandwidth's 8 kHz μ-law wire audio to the pipeline rate.
2. ``LLMContextAggregatorPair`` is constructed with ``realtime_service_mode=True``,
   which pipecat recommends for speech-to-speech services (context writes are
   driven by the content stream rather than discrete turn frames). There is no
   VAD analyzer in the pipeline: the service broadcasts the user-speaking
   frames off OpenAI's server-side speech events.

Run::

    uv sync
    cp env.example .env  # OPENAI_API_KEY + Bandwidth credentials are enough
    uv run python bot_realtime.py

Then point an ngrok tunnel at port 7860 and configure your Bandwidth
application's voice webhook to ``https://<your-ngrok-host>/``.

Unlike ``bot.py``, there is no Basic Auth on ``POST /``. The webhook still
mints a one-time token and the trusted call/account IDs are carried through
the token mapping (not the WS ``start`` metadata), but the POST itself is
unauthenticated — matching ``bot_outbound.py``'s trust model. For a
production inbound deployment, re-add webhook Basic Auth as ``bot.py`` does.
"""

import asyncio
import json
import os
import secrets
import time

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, WebSocket, status
from fastapi.responses import HTMLResponse
from loguru import logger
from pipecat.frames.frames import LLMRunFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
)
from pipecat.services.openai.realtime.llm import OpenAIRealtimeLLMService
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)

from pipecat_bandwidth import BandwidthFrameSerializer

load_dotenv(override=True)

PROXY_HOST = os.getenv("PROXY_HOST", "")  # e.g. "abcd-12-34-56-78.ngrok-free.app"
PORT = int(os.getenv("PORT", "7860"))

# How long an issued token is valid before Bandwidth must connect to /ws.
TOKEN_TTL_SECONDS = 60

app = FastAPI()


# In-memory token → (call_id, account_id, expires_at) store. Single-process
# only; for multi-worker deployments back this with Redis or similar.
_pending_calls: dict[str, tuple[str, str, float]] = {}
_pending_calls_lock = asyncio.Lock()


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
    """Bandwidth voice webhook. Mint a token bound to the body's call IDs."""
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
        params=BandwidthFrameSerializer.InputParams(
            # OpenAI Realtime emits 24 kHz PCM; send it to Bandwidth as PCM
            # rather than down-converting to μ-law for better fidelity.
            outbound_encoding="PCM",
            outbound_pcm_sample_rate=24000,
        ),
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
    """Build and run the speech-to-speech pipeline for one call."""
    llm = OpenAIRealtimeLLMService(
        api_key=os.getenv("OPENAI_API_KEY"),
        settings=OpenAIRealtimeLLMService.Settings(
            system_instruction=(
                "You are a helpful assistant on a phone call. Keep responses "
                "concise and conversational. Avoid special characters since "
                "your output is converted to audio."
            ),
        ),
    )

    # realtime_service_mode=True is pipecat's recommended configuration for
    # speech-to-speech services: context writes are driven by the content
    # stream rather than discrete turn frames. No VAD analyzer is needed — the
    # OpenAI Realtime API does server-side turn detection and the service
    # broadcasts the UserStartedSpeaking / UserStoppedSpeaking frames itself.
    context = LLMContext()
    user_aggregator, assistant_aggregator = LLMContextAggregatorPair(
        context,
        realtime_service_mode=True,
    )

    pipeline = Pipeline(
        [
            transport.input(),
            user_aggregator,
            llm,
            transport.output(),
            assistant_aggregator,
        ]
    )

    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            audio_in_sample_rate=24000,
            audio_out_sample_rate=24000,
            enable_metrics=True,
            enable_usage_metrics=True,
        ),
    )

    @transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        context.add_message({"role": "user", "content": "Please introduce yourself to the caller."})
        await task.queue_frames([LLMRunFrame()])

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        await task.cancel()

    runner = PipelineRunner(handle_sigint=False)
    await runner.run(task)


if __name__ == "__main__":
    import uvicorn

    if not PROXY_HOST:
        logger.warning(
            "PROXY_HOST is not set — the BXML response will reference an empty "
            "host. Set PROXY_HOST in .env to your public ngrok hostname."
        )

    uvicorn.run(app, host="0.0.0.0", port=PORT)
