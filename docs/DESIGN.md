# mod_ws_media — Routable Media Bus (Design & Protocol v1)

> **Status: PROPOSAL / not yet implemented.**
> This document describes the intended, general-purpose ("commercial-grade")
> shape of the module. The *as-built* wire format of the current code is in
> [`PROTOCOL.md`](PROTOCOL.md) (call it v0). This file (v1) is the target we
> design toward; it is written to be reviewed and iterated **before** touching
> code.

---

## 1. Vision & positioning

`mod_ws_media` is a **routable, bidirectional media bypass bus** for FreeSWITCH.

For any channel (leg), it can:

- **fork/copy** that leg's audio out to an external *stream service* (any
  implementation, any language) over a WebSocket — for ASR, recording,
  analytics, keyword spotting, etc.; and/or
- **inject** audio that the stream service sends back into a chosen leg — for
  real-time translation, TTS prompts, voice conversion, noise cancellation,
  agent-assist whisper, etc.

The defining capability over a plain "audio fork" module: **capture and
injection are decoupled and routable**. Audio captured from leg A may, after
processing, be injected into A itself, into its peer B, or into *a leg in a
different, already-existing call* — and the module needs no special logic for
any of it.

Reference points in the market (all WebSocket-based): FreeSWITCH
mod_audio_fork / mod_audio_stream, Twilio Media Streams, Deepgram/Google/AWS/
Azure streaming, and the Chinese ASR clouds (Alibaba/Tencent/iFlytek/Volcano).
v1 aims to match that interoperability bar.

## 2. Core principle: a dumb pipe + an external router

> **One leg = one bidirectional WebSocket connection.**
> **Uplink = audio captured from that leg. Downlink = audio to inject into that leg.**
> **Who-goes-where is decided by the stream service, by choosing which leg's
> connection to write the processed audio to.**

Consequences:

- The module keeps **no routing table and no cross-call logic**. Each connection
  is bound to exactly one leg for both directions.
- All routing/decision intelligence (including cross-call) lives in the stream
  service — which is exactly where "whoever implements the service" wants it.
- Cross-call injection "just works": the target leg only needs to have
  `ws_media` attached (hence a live connection the service can address).

An optional advanced mode (multiplexing many legs over one connection, with a
per-message target selector) is described in §16; **per-leg connections are the
default** because they keep the module simple and give per-stream fault
isolation.

## 3. Transport: why WebSocket

| Transport | Verdict | Rationale |
|---|---|---|
| **WebSocket (ws/wss)** | **Default & required** | Ubiquitous across the media-AI ecosystem; built-in framing / ping-pong / close / control frames; traverses L7 LBs, reverse proxies, API gateways, TLS termination; handshake carries auth/headers/path; bidirectional. |
| Raw TCP | Not worth it | Saves only the 2–14 B frame header + masking XOR (negligible vs a 320+ B audio frame) while discarding all interop and forcing you to reinvent framing/keepalive/reconnect/auth. |
| UDP / custom | Wrong tool here | Would require reinventing RTP (sequencing, jitter buffer, loss handling, NAT traversal); ASR/analytics backends do not consume UDP; for these workloads *reliable & ordered* matters more than shaving latency. Only relevant for a pure media *relay* to another media endpoint — a different product. |
| gRPC (HTTP/2) | Possible future 2nd transport | Multiplexing, backpressure, schema; some enterprise backends prefer it. Additive, not a replacement. |

**Decision:** WebSocket is the core transport. Keep the transport layer
abstracted so a gRPC backend can be added later without touching capture/inject.

## 4. Concepts

**Attach modes** (per leg):

- `tap` — capture only (fork/copy out, no return). ASR, recording, analytics.
- `sink` — inject only (play returned audio into this leg).
- `duplex` — both (capture out *and* inject in). Translation, voice conversion.

**Capture selector** — `in = read | write | mixed | stereo`:

| `in` | Captures | Channels | Notes |
|---|---|---|---|
| `read` | this leg's own party (its microphone — what it *says*) | 1 | mono, "self" |
| `write` | the far party (what this leg *hears*) | 1 | mono, "peer" |
| `mixed` | both, summed into one channel | 1 | can't separate speakers; half the bytes |
| `stereo` | both, kept separate | 2 | left = read (self), right = write (peer); speaker-separated |

**Inject selector** — `out = read | write`:

- `write` — inject into the WRITE stream → **this leg hears** the injected audio.
- `read` — inject into the READ stream → **the far party hears** it (as if this leg had said it).

## 5. Connection model

- Each `ws_media start` on a leg opens **one WebSocket connection** to the
  configured (or per-call) URL.
- That connection is **bidirectional**:
  - **module → service** carries the captured audio (per `in`), plus text
    control frames (`start`, `stop`, `mark`).
  - **service → module** carries audio to inject (per `out`), plus text control
    frames (`clear`, `mark`, `kill`).
- The connection lives for the duration of the attach (call leg). One leg → one
  connection; both legs of a call attached → two connections; N participating
  legs → N connections. No multiplexing by default.

## 6. Attribution: declare, don't guess

FreeSWITCH fixes the *direction→content* mapping regardless of caller/callee
(verified against `switch_core_media_bug_read` and by live call):

- **READ** of a leg = the party **on this leg** (its microphone) = what the leg
  *says* → the "self" side.
- **WRITE** of a leg = the **far** party's audio (what the leg *hears*) → the
  "peer" side.
- In `stereo`, **left = read (self), right = write (peer)** (swappable via
  `SMBF_STEREO_SWAP`).

The only thing the module cannot know by itself is the *role* of the leg it was
attached to (agent vs customer vs "speaker A"). So the **controller declares
it** via a channel variable / command metadata, and the module stamps explicit
labels into the `start` frame. The service reads the labels and never guesses —
correct whether you attach to the caller or the callee.

## 7. Wire protocol v1

### 7.1 Handshake

Standard RFC 6455 upgrade to the configured URL. The handshake MAY carry:

- `Authorization: Basic|Bearer …` (from config/metadata),
- arbitrary custom headers (e.g. an API key),
- query parameters,
- optional mTLS client cert.

### 7.2 `start` (module → service, text/JSON, first frame)

```json
{
  "event": "start",
  "version": "1",
  "call_id": "<opaque id from controller, e.g. a CDR/conversation id>",
  "leg_uuid": "<freeswitch channel uuid>",
  "attach_mode": "duplex",
  "media_format": { "encoding": "L16", "sample_rate": 16000, "channels": 2, "ptime": 20 },
  "capture": {
    "mode": "stereo",
    "tracks": [
      { "ch": 0, "source": "read",  "role": "agent" },
      { "ch": 1, "source": "write", "role": "customer" }
    ]
  },
  "inject": {
    "enabled": true,
    "target": "read",
    "expects": { "encoding": "L16", "sample_rate": 16000, "channels": 1 }
  },
  "custom": { "tenant": "...", "agent_id": "...", "...": "..." }
}
```

- `encoding` is always `L16` (signed 16-bit little-endian PCM) in v1.
- `sample_rate` is the negotiated rate the module will **send and expects to
  receive** (see §9 resampling).
- `role`/`tracks` come from controller-supplied metadata (§6).
- `custom` is passed through verbatim from the attach command / channel vars.

### 7.3 Media frames (both directions, binary)

- **Binary** WS frame = raw PCM in the declared `media_format`.
  - Capture direction: interleaved per `capture` (`stereo` → `[L R L R …]`).
  - Inject direction: PCM matching `inject.expects`.
- A binary message is **not** guaranteed to equal one 20 ms frame; use
  `sample_rate`/`channels`/`ptime` for validation, not as a fixed length.
- **Optional framed mode** (negotiated with `media_header=true`): prefix each
  binary message with a small fixed header so a single connection can carry
  timestamps/sequence and (future) multiple tracks:

  ```
  offset 0  : uint8   magic/version (0xA1)
  offset 1  : uint8   flags
  offset 2-3: uint16  track id (0 = default)
  offset 4-7: uint32  media timestamp (ms)
  offset 8-9: uint16  sequence number
  offset 10+: PCM payload
  ```

  Default is **headerless raw PCM** for maximum interop.

### 7.4 Control (service → module, text/JSON)

| Event | Meaning |
|---|---|
| `{"event":"clear"}` | Drop everything queued for injection (barge-in / cancel). |
| `{"event":"mark","name":"utt-42"}` | Marker; the module emits a matching `mark` back (and/or an ESL event) when that point has been played out. |
| `{"event":"kill"}` | Stop this stream (module tears the attach down). |

Injected audio itself is sent as **binary** frames (symmetric with capture) —
not base64-in-JSON — to keep it efficient.

### 7.5 `stop` + close

On teardown the module sends `{"event":"stop","call_id":…,"reason":…}` and then
a proper WebSocket **Close** frame before shutting the socket.

## 8. Routing recipes

Assume a stream service that holds each leg's connection keyed by `leg_uuid` /
`call_id`.

- **Real-time translation, A↔B (bidirectional):** attach `duplex` on both A and
  B, `in=write` (capture each speaker's own voice), `out=read` (each hears the
  injected result). Service: A-conn uplink → translate → **send on B-conn**
  (B hears A translated); B-conn uplink → translate → send on A-conn. For a pure
  translation experience, do **not** natively bridge the raw audio (or mute it)
  so each side hears only translations.
- **Hear-self (confirmation / self-monitor):** service returns the processed
  audio on the **same** leg's connection.
- **Inject into a leg in another call:** that leg just needs `ws_media`
  attached; the service sends on its connection. No module change.
- **Fork only (ASR / recording / analytics):** `tap` mode; service never sends
  audio back.

## 9. Audio format & resampling

- Media-bug audio is always decoded **L16**; sample rate follows the channel
  codec (`actual_samples_per_second`, so G.722 reports 16 k, not 8 k).
- The module resamples **to** the negotiated `sample_rate` on capture and
  **from** it on inject, so the service can standardize on e.g. 16 kHz while the
  target leg runs 8 kHz telephony.
- Inject audio is resampled to the **target leg's** codec rate before it is
  written into that leg.

## 10. Timing, latency & ducking (the hard parts)

These are inherent to "process then re-inject", especially translation — the
module provides the *transport + injection primitive*; the **policy is the
service's / dialplan's** job:

- **Latency & granularity:** STT→MT→TTS is seconds-scale and **utterance-based**,
  not frame-synchronous. Injected audio arrives late and in chunks; it cannot be
  a frame-for-frame replacement of a live stream.
- **Original-audio suppression / ducking:** to make B hear only the translation,
  the raw A→B path must be muted or ducked. `out=read` uses replace semantics
  (inject when the buffer has audio, else pass through), but for clean
  translation you typically **don't bridge the raw audio at all** and let the
  service be the sole audio path.
- **Underrun:** when no injected audio is ready, define what plays (silence /
  comfort noise / pass-through).
- **Clock/skew:** resample to the target rate; never assume both ends share a
  clock.

## 11. Backpressure, reconnect, bypass

- **Backpressure:** bounded per-stream buffers with a drop-oldest policy that
  favors latency over completeness (`max-queue-size` / `drop-threshold`).
- **Reconnect:** on failure, reconnect with the configured interval up to
  `max-retry-count`; connections are established off the call-setup path.
- **Bypass:** after exhausting retries (or on excessive drop rate) the leg
  enters *bypass* (audio flows natively, module stops streaming). Bypass is
  **recoverable**: after `bypass-recovery-interval` the module retries the
  backend.
- **Resume (future):** on reconnect, report the last sequence number so a
  service can resume cleanly.

## 12. Configuration & control API

Per-call configuration is a hard requirement for a general module; global
config only supplies defaults.

Command (per call):

```
uuid_ws_media <uuid> start <url>
      mode=<tap|sink|duplex>
      in=<read|write|mixed|stereo>
      out=<read|write>
      [rate=16000] [meta='{...json...}']

uuid_ws_media <uuid> stop
uuid_ws_media <uuid> pause | resume
uuid_ws_media <uuid> stats
```

Or via channel variables before `ws_media_start` (dialplan-friendly):
`ws_media_url`, `ws_media_mode`, `ws_media_in`, `ws_media_out`, `ws_media_rate`,
`ws_media_role`, `ws_media_call_id`, and arbitrary `ws_media_meta_*`.

## 13. Security

- `wss://` via TLS; server-cert verification (`ws-ssl-verify`), SNI, custom CA,
  optional mTLS client cert.
- Auth: HTTP Basic, Bearer token, and arbitrary custom headers (for backend API
  keys). Never log secrets.

## 14. Observability

- ESL CUSTOM events: `start`, `stop`, `connected`/`disconnected` (per
  direction), `error`, `mark`.
- `uuid_ws_media <uuid> stats`: frames/bytes sent & received, drops, drop rate,
  reconnects, bypass state.
- Counters are updated with atomics so readings are consistent across threads.

## 15. Relationship to the current implementation & migration

The current code (v0, see `PROTOCOL.md`) already has: a hand-rolled RFC 6455
client, per-direction connections, an `init` frame, binary L16, serial (replace)
and parallel (copy) modes, drop policy, reconnect, recoverable bypass, atomic
stats, TLS (method/SNI/optional verify), and CLOSE-time cleanup.

To reach v1, in rough dependency order:

1. **Per-call config** — accept URL/mode/`in`/`out`/rate/metadata from the
   command and channel vars (biggest gap today; config is global-only).
2. **Decouple capture from inject** — replace the fixed "capture-A/inject-A"
   coupling with independent `in` (read|write|mixed|stereo) and `out`
   (read|write), plus `tap`/`sink`/`duplex` modes.
3. **`start`/`stop` control frames + graceful Close** — supersede the bare
   `init` packet; emit a Close frame on teardown.
4. **Resampling** — capture-to-rate and inject-to-target-rate.
5. **Role/label metadata** in `start` (declare-don't-guess).
6. **Auth/security extensions** — Bearer, custom headers, mTLS.
7. **Nice-to-haves** — `pause`/`resume`, `mark`, `stats` API, optional binary
   media header, reconnect-resume.

Backward compatibility: v1 can keep a "raw/legacy" mode equivalent to v0 for
existing servers, selected by config.

## 16. Open questions / roadmap

- **Multiplexing:** one connection carrying many legs with a per-message
  `track`/target id, vs. the default one-connection-per-leg. Multiplexing lowers
  connection count at high scale but adds head-of-line-blocking risk and a
  demux/routing protocol; only pursue if connection count is proven to hurt.
- **gRPC transport** as an additive backend.
- **Per-session config snapshot** (also closes the reload-safety gap): capture
  all config into the session at attach so worker threads never read globals.
- **Codec passthrough** (send encoded RTP payload instead of L16) for bandwidth
  — niche; L16 is the ASR-friendly default.
- **Send-only stereo variant** using `SMBF_READ_STREAM|SMBF_WRITE_STREAM|
  SMBF_STEREO` + `switch_core_media_bug_read` (FS synchronizes the two
  directions for you) — the natural implementation of `in=stereo, mode=tap`.
