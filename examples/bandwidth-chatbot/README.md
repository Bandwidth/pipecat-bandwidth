# Bandwidth Chatbot Example

A minimal Pipecat voice bot that answers inbound Bandwidth Programmable Voice
calls. The bot transcribes the caller with Deepgram, generates a response with
an OpenAI model, and speaks back using Cartesia.

## What this example shows

- Handling Bandwidth's voice webhook with a `<StartStream>` BXML response
- Accepting the bidirectional media-stream WebSocket
- Wiring `BandwidthFrameSerializer` into a `FastAPIWebsocketTransport`
- A simple STT → LLM → TTS Pipecat pipeline with Silero VAD for turn detection

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/)
- [ngrok](https://ngrok.com) (or any tunnel that gives you a public HTTPS URL)
- Bandwidth Programmable Voice account with:
  - A purchased phone number assigned to a Voice Application
  - API credentials (OAuth 2.0 client ID + secret)
- API keys for OpenAI, Deepgram, and Cartesia

## Setup

```sh
cd examples/bandwidth-chatbot
uv sync
cp env.example .env
# fill in the values in .env
```

In a separate terminal, start ngrok:

```sh
ngrok http 7860
```

Note the `https://...ngrok-free.app` hostname (without the scheme) and put it
in `.env` as `PROXY_HOST`.

In your Bandwidth Voice Application configuration:

- Set the Voice Webhook URL to `https://<your-ngrok-host>/`.
- Set HTTP Basic Auth on the webhook using the same
  `BANDWIDTH_WEBHOOK_USERNAME` / `BANDWIDTH_WEBHOOK_PASSWORD` you put in
  `.env`. The bot rejects unauthenticated POSTs (returns `401`) and refuses
  to start if these env vars are unset (returns `503`).

## Run

```sh
uv run python bot.py
```

Call your Bandwidth number. You should hear the bot introduce itself and you
can start a conversation.

## How it works

1. **Inbound call.** Bandwidth POSTs to `/` (HTTP Basic Auth) when a call
   comes in. The bot reads `callId` and `accountId` from the authenticated
   webhook body, mints a one-time correlation token, and responds with a
   `<StartStream>` BXML pointing at `wss://<host>/ws/<token>`.
2. **WebSocket handshake.** Bandwidth opens a WebSocket to `/ws/<token>`.
   The bot validates the token, looks up the trusted call/account IDs from
   server-side state, and constructs `BandwidthFrameSerializer` with those
   IDs — **not** with whatever the WebSocket's `start` event metadata
   claims.
3. **Pipeline runs.** Inbound μ-law audio is decoded, transcribed, fed to the
   LLM, the response is synthesized to audio, and sent back to Bandwidth as
   `playAudio` events. Interruptions emit a `clear` event so the bot stops
   talking immediately when the caller speaks.
4. **Hang up.** When the pipeline ends, the serializer terminates the call via
   the Bandwidth Voice API using the trusted call ID.

## Why the token-in-URL?

The auto-hang-up path inside `BandwidthFrameSerializer` POSTs to the Voice
API using the operator's OAuth credentials. If the call ID it operates on
came from an unauthenticated WebSocket frame, anyone who can reach `/ws`
could feed the bot an arbitrary call ID in the operator's account and
trigger a hang-up against a live call. Trusting only the (Basic-Auth'd)
webhook body for `callId`/`accountId` and binding them to the WebSocket via
a server-issued correlation token closes that hole.

For production deployments, also consider:

- Verifying Bandwidth's webhook signature in addition to (or instead of)
  Basic Auth.
- IP-allowlisting Bandwidth's egress ranges at your ingress.
- Backing the in-memory token store with Redis or similar if you run more
  than one worker.

## OpenAI Realtime variant (`bot_realtime.py`)

`bot_realtime.py` runs the same inbound flow on OpenAI's Realtime
speech-to-speech model. STT, LLM, and TTS collapse into one
`OpenAIRealtimeLLMService`, so on the AI side it needs **only an
`OPENAI_API_KEY`** — no Deepgram, no Cartesia.

```sh
uv run python bot_realtime.py
```

How it differs from `bot.py`:

- **One service, not three.** The pipeline is just
  `transport.input() → user_agg → llm → transport.output() → assistant_agg`.
- **24 kHz PCM both directions.** The serializer is configured with
  `outbound_encoding="PCM"`, `outbound_pcm_sample_rate=24000`, and the
  pipeline runs at 24 kHz to match the Realtime model.
- **No VAD analyzer.** OpenAI does server-side turn detection and the service
  emits the user-speaking frames itself. The VAD is tuned for telephony
  (`threshold=0.8`, `silence_duration_ms=800`, far-field noise reduction) so
  line/acoustic echo of the bot's own speech doesn't self-interrupt it.
- **No webhook Basic Auth.** Unlike `bot.py`, `POST /` is unauthenticated; the
  one-time WS token still carries server-trusted call IDs. For a production
  inbound deployment, re-add webhook Basic Auth as `bot.py` does.

## Notes

- DTMF is captured by Bandwidth's `<Gather>` BXML verb on a separate webhook
  rather than being delivered over the media stream. If you need DTMF, wire it
  in your application's webhook handler — not in the serializer.
- The serializer also supports linear PCM at 16/24 kHz outbound for noticeably
  better TTS quality than μ-law. Pass
  `outbound_encoding="PCM"` and `outbound_pcm_sample_rate=24000` via
  `BandwidthFrameSerializer.InputParams`.
