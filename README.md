# pipecat-bandwidth

A Pipecat community integration that lets you use [Bandwidth Programmable
Voice](https://dev.bandwidth.com/docs/voice/) as the telephony layer for your
Pipecat voice bots.

This package provides `BandwidthFrameSerializer`, a `FrameSerializer` you plug
into a `FastAPIWebsocketTransport` to handle Bandwidth's bidirectional
WebSocket media stream protocol.

> Maintained by [Bandwidth](https://www.bandwidth.com).

## What it does

- Decodes Bandwidth's inbound μ-law audio (8 kHz) into Pipecat audio frames.
- Encodes outbound audio as either μ-law or **linear PCM at 8/16/24 kHz**.
  PCM at 24 kHz noticeably improves TTS quality compared to μ-law.
- Handles interruptions by emitting Bandwidth's `clear` event, so the bot
  stops talking immediately when the caller speaks.
- Auto hangs up the call via the Bandwidth Voice API on `EndFrame` or
  `CancelFrame`, using OAuth 2.0 client_credentials.

## Installation

```sh
pip install pipecat-bandwidth
```

Or with [uv](https://docs.astral.sh/uv/):

```sh
uv add pipecat-bandwidth
```

## Usage

```python
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)
from pipecat_bandwidth import BandwidthFrameSerializer

# stream_id, call_id, account_id come from Bandwidth's first WebSocket message
# (the "start" event's metadata block).
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
```

For higher-fidelity outbound audio, configure linear PCM:

```python
from pipecat_bandwidth import BandwidthFrameSerializer

serializer = BandwidthFrameSerializer(
    stream_id=stream_id,
    call_id=call_id,
    account_id=account_id,
    client_id=os.getenv("BANDWIDTH_CLIENT_ID"),
    client_secret=os.getenv("BANDWIDTH_CLIENT_SECRET"),
    params=BandwidthFrameSerializer.InputParams(
        outbound_encoding="PCM",
        outbound_pcm_sample_rate=24000,
    ),
)
```

## Example

A complete end-to-end example lives in
[`examples/bandwidth-chatbot`](./examples/bandwidth-chatbot). It shows a
single-file FastAPI server that:

1. Returns a `<StartStream>` BXML response on Bandwidth's voice webhook.
2. Accepts the bidirectional WebSocket and reads Bandwidth's `start` event
   for the stream/call/account IDs.
3. Runs a Deepgram (STT) → OpenAI (LLM) → Cartesia (TTS) Pipecat pipeline.

See the example's [README](./examples/bandwidth-chatbot/README.md) for setup
and run instructions.

## DTMF

Bandwidth does **not** deliver DTMF over the media-stream WebSocket. DTMF is
captured by the BXML `<Gather>` verb and posted to a separate webhook. Wire
DTMF handling in your application's webhook handler — not in the serializer.

## Compatibility

- Tested with Pipecat **v1.1.0**.
- Python 3.11, 3.12.

## Links

- [Bandwidth StartStream BXML reference](https://dev.bandwidth.com/docs/voice/programmable-voice/bxml/startStream/)
- [Bandwidth Voice API](https://dev.bandwidth.com/apis/voice/)
- [Pipecat](https://github.com/pipecat-ai/pipecat)
- [Pipecat Community Integrations](https://docs.pipecat.ai/server/services/community-integrations)

## License

BSD 2-Clause. See [LICENSE](./LICENSE).

## Contributing

Issues and PRs are welcome. For larger changes, please open an issue first to
discuss what you'd like to change.
