#
# Copyright (c) 2026, Bandwidth Inc.
#
# SPDX-License-Identifier: BSD-2-Clause
#

"""Foundational Bandwidth + Pipecat example.

A single-file FastAPI application that handles a Bandwidth Programmable Voice
inbound call:

1. ``POST /`` returns a StartStream BXML document, telling Bandwidth to open a
   bidirectional WebSocket to ``/ws``.
2. ``/ws`` accepts the WebSocket, reads Bandwidth's first ``start`` event to
   extract stream/call/account IDs, and runs a Pipecat pipeline:

       Bandwidth WS  ->  Deepgram STT  ->  OpenAI LLM  ->  Cartesia TTS  ->  Bandwidth WS

Run::

    uv sync
    cp env.example .env  # fill in API keys + Bandwidth credentials
    uv run python bot.py

Then point an ngrok tunnel at port 7860 and configure your Bandwidth
application's voice webhook to ``https://<your-ngrok-host>/``.
"""

import json
import os

from dotenv import load_dotenv
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import HTMLResponse
from loguru import logger
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import LLMRunFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
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

from pipecat_bandwidth import BandwidthFrameSerializer

load_dotenv(override=True)

PROXY_HOST = os.getenv("PROXY_HOST", "")  # e.g. "abcd-12-34-56-78.ngrok-free.app"
PORT = int(os.getenv("PORT", "7860"))

app = FastAPI()


@app.post("/")
async def inbound_call(request: Request) -> HTMLResponse:
    """Bandwidth voice webhook. Returns BXML telling Bandwidth to open a
    bidirectional media-stream WebSocket back to this server.
    """
    bxml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <StartStream destination="wss://{PROXY_HOST}/ws" mode="bidirectional" tracks="inbound"/>
  <Pause duration="86400"/>
</Response>"""
    return HTMLResponse(content=bxml, media_type="application/xml")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """Handle the Bandwidth media-stream WebSocket and run the bot pipeline."""
    await websocket.accept()

    # Bandwidth's first frame on the WebSocket is a "start" event whose
    # `metadata` block carries the stream/call/account IDs we need to wire up
    # the serializer (and to call the Voice API for hang-up).
    first = await websocket.receive_text()
    start_event = json.loads(first)
    metadata = start_event.get("metadata", {})

    stream_id = metadata["streamId"]
    call_id = metadata["callId"]
    account_id = metadata["accountId"]

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

    task = PipelineTask(
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
