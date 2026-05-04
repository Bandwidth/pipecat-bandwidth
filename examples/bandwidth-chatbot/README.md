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

In your Bandwidth Voice Application configuration, set the Voice Webhook URL
to `https://<your-ngrok-host>/`.

## Run

```sh
uv run python bot.py
```

Call your Bandwidth number. You should hear the bot introduce itself and you
can start a conversation.

## How it works

1. **Inbound call.** Bandwidth POSTs to `/` when a call comes in. The bot
   responds with a `<StartStream>` BXML document pointing at the WebSocket
   endpoint.
2. **WebSocket handshake.** Bandwidth opens a WebSocket to `/ws` and sends a
   `start` event whose `metadata` block carries `streamId`, `callId`, and
   `accountId`. The bot reads this first frame to construct
   `BandwidthFrameSerializer`.
3. **Pipeline runs.** Inbound μ-law audio is decoded, transcribed, fed to the
   LLM, the response is synthesized to audio, and sent back to Bandwidth as
   `playAudio` events. Interruptions emit a `clear` event so the bot stops
   talking immediately when the caller speaks.
4. **Hang up.** When the pipeline ends, the serializer terminates the call via
   the Bandwidth Voice API.

## Notes

- DTMF is captured by Bandwidth's `<Gather>` BXML verb on a separate webhook
  rather than being delivered over the media stream. If you need DTMF, wire it
  in your application's webhook handler — not in the serializer.
- The serializer also supports linear PCM at 16/24 kHz outbound for noticeably
  better TTS quality than μ-law. Pass
  `outbound_encoding="PCM"` and `outbound_pcm_sample_rate=24000` via
  `BandwidthFrameSerializer.InputParams`.
