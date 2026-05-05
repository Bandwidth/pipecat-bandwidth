# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-05-04

### Added

- Initial release of `BandwidthFrameSerializer`, a Pipecat
  [`FrameSerializer`](https://github.com/pipecat-ai/pipecat) for Bandwidth
  Programmable Voice bidirectional WebSocket media streams.
- Inbound μ-law (8 kHz) decoding to Pipecat audio frames.
- Outbound encoding for both μ-law (broadest compatibility) and linear PCM at
  8/16/24 kHz (higher TTS fidelity).
- Interruption handling via Bandwidth's `clear` event.
- Auto hang-up on `EndFrame` / `CancelFrame` using Bandwidth's Voice API,
  authenticated with OAuth 2.0 client_credentials.
- `examples/bandwidth-chatbot` — single-file FastAPI bot showing inbound-call
  webhook handling and a Deepgram → OpenAI → Cartesia pipeline.
- Tested with Pipecat v1.1.0.
