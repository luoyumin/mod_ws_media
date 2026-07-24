# mod_ws_media

A FreeSWITCH module that **taps a call leg's audio and streams it to an external
service over a WebSocket** in real time. The audio is decoded **L16 PCM**, so the
server never deals with the RTP codec. Built for real-time ASR / transcription,
recording, and audio analytics.

Self-contained WebSocket client (RFC 6455) on top of OpenSSL — **no
libwebsockets, no gRPC, no extra runtime dependencies** beyond OpenSSL.

> **v1.0 — tap only.** This version captures/forks audio out; it is **completely
> non-intrusive** (read-only, never modifies or injects audio into the call).
> Injection and cross-leg routing are on the roadmap (see
> [`docs/DESIGN.md`](docs/DESIGN.md)). Validated on a live call.

---

## Features

- **Fork/copy a leg's audio out** over one WebSocket connection per leg —
  completely non-intrusive to the call.
- **Per-call configuration** — target URL, capture mode, role and metadata are
  set per call (command args / channel variables); the global config is only
  defaults.
- **Capture modes** (`in=`):
  - `read` — this leg's own party (its mic; what it *says*) — mono, "self"
  - `write` — the far party (what this leg *hears*) — mono, "peer"
  - `mixed` — both parties summed into one mono channel
  - `stereo` — both parties separated: **left = read (self), right = write
    (peer)** — natural speaker separation
- **Codec-agnostic** — media-bug audio is always decoded L16, so it works the
  same for PCMU/PCMA/G.722/Opus. Sample rate is the channel's native rate.
- **Graceful degradation** — automatic reconnect, and a recoverable *bypass*
  mode (the call continues untouched when the backend is unreachable).
- **TLS** (`wss://`) with optional certificate verification and SNI.
- **Observability** — FreeSWITCH CUSTOM events for start/stop/connect/error.

## How it works

The module attaches a FreeSWITCH media bug (stream tap) to the leg. In the
`READ` callback it pulls decoded, already-synchronized L16 frames via
`switch_core_media_bug_read` and streams them to the WebSocket server. Direction
semantics (fixed by FreeSWITCH, verified by live test):

- **READ** = the party on this leg (its microphone) — what the leg *says* → "self".
- **WRITE** = the far party (what the leg *hears*) → "peer".
- In `stereo`: **left = read (self), right = write (peer)**.

Routing is intentionally kept out of the module: it is a faithful per-leg pipe.

## Requirements

- FreeSWITCH **1.10.x** (headers to build against).
- OpenSSL (libssl + libcrypto).
- A C compiler + `make`; `pkg-config` recommended.

The channel must have a **real media path** — the media bug cannot attach to a
channel in `bypass_media`/`proxy_media` mode or one that is only `park`ed. Use
`echo`, `playback`, `bridge`, etc.

## Build & install

### Out-of-tree, against an installed FreeSWITCH (recommended)

```bash
make
sudo make install          # mod_ws_media.so -> module dir; ws_media.conf.xml -> autoload_configs (if absent)
```

Point at a specific FreeSWITCH prefix if pkg-config isn't set up:

```bash
make              FREESWITCH_PATH=/usr/local/freeswitch
sudo make install FREESWITCH_PATH=/usr/local/freeswitch
```

On **macOS** with Homebrew OpenSSL:

```bash
export PKG_CONFIG_PATH="$(brew --prefix openssl@3)/lib/pkgconfig:$PKG_CONFIG_PATH"
make
```

### In-tree, like the bundled modules

```bash
cp -r mod_ws_media <freeswitch-src>/src/mod/applications/mod_ws_media
echo "applications/mod_ws_media" >> <freeswitch-src>/modules.conf
cd <freeswitch-src> && make
# or: cd src/mod/applications/mod_ws_media && make && make install
```

`Makefile.am` is the in-tree build file; the plain `Makefile` is out-of-tree.

## Load

```bash
fs_cli -x "load mod_ws_media"        # or: reload mod_ws_media
fs_cli -x "module_exists mod_ws_media"   # -> true
```

## Usage

### Dialplan app

```xml
<action application="ws_media_start" data="ws://127.0.0.1:8080/media in=stereo role=agent"/>
<!-- ... call proceeds ... -->
<action application="ws_media_stop"/>
```

### API (on a live call)

```bash
uuid_ws_media <uuid> start ws://127.0.0.1:8080/media in=stereo role=agent call_id=abc123
uuid_ws_media <uuid> stop
```

### Channel variables (dialplan-friendly)

```
uuid_setvar <uuid> ws_media_url     ws://127.0.0.1:8080/media
uuid_setvar <uuid> ws_media_in      stereo
uuid_setvar <uuid> ws_media_role    agent
uuid_setvar <uuid> ws_media_call_id abc123
uuid_setvar <uuid> ws_media_meta    {"tenant":"t1"}
uuid_ws_media <uuid> start
```

### Parameters

| Command arg | Channel variable | Meaning | Default |
|---|---|---|---|
| `ws://…` / `wss://…` | `ws_media_url` | Target URL (host/port/path) | conf `ws-host/port/path` |
| `in=read\|write\|mixed\|stereo` | `ws_media_in` | Capture mode | `read` |
| `role=…` | `ws_media_role` | Role label of this leg (into the `start` frame) | `self` |
| `call_id=…` | `ws_media_call_id` | Your business id (into the `start` frame) | the leg's FS uuid |
| — | `ws_media_meta` | Arbitrary JSON passed through to the server | `{}` |

## Wire protocol

On connect the module sends one JSON **text** frame, then streams **binary** L16
PCM. (Full/target protocol incl. injection is specified in
[`docs/DESIGN.md`](docs/DESIGN.md).)

```json
{
  "event": "start",
  "version": "1",
  "call_id": "abc123",
  "leg_uuid": "<freeswitch channel uuid>",
  "attach_mode": "tap",
  "media_format": { "encoding": "L16", "sample_rate": 8000, "channels": 2, "ptime": 20 },
  "capture": {
    "mode": "stereo",
    "tracks": [
      { "ch": 0, "source": "read",  "role": "agent" },
      { "ch": 1, "source": "write", "role": "peer" }
    ]
  },
  "custom": { "tenant": "t1" }
}
```

Binary frames: raw signed 16-bit little-endian PCM.
- `sample_rate` = the channel's native rate (e.g. G.722 → 16000, Opus → 48000).
- channels: `read`/`write`/`mixed` = 1; `stereo` = 2 (interleaved `[L R L R …]`,
  left = read/self, right = write/peer).
- A binary message is **not** guaranteed to be exactly one 20 ms frame; treat it
  as a byte stream.

## Events

CUSTOM events, subclass prefix `ws_media::`:
`start`, `stop`, `connected`, `disconnected`, `error`.

```
fs_cli> /events plain CUSTOM ws_media::start ws_media::stop ws_media::error
```

## Configuration (`autoload_configs/ws_media.conf.xml` — defaults only)

| Param | Default | Meaning |
|---|---|---|
| `ws-host` / `ws-port` / `ws-path` | `localhost` / `8080` / `/media` | Default target |
| `ws-ssl` | `false` | Use TLS (`wss://`) |
| `ws-ssl-verify` | `false` | Verify server certificate when TLS is on |
| `ws-auth-user` / `ws-auth-pass` | – | HTTP Basic auth (optional) |
| `ws-query-params` | – | Extra query string on the handshake URL |
| `max-queue-size` | `8192` | Per-buffer byte cap before dropping oldest |
| `drop-threshold` | `4096` | Target size to trim back to when dropping |
| `reconnect-interval` | `5` | Seconds between reconnects (and socket I/O timeout) |
| `max-retry-count` | `3` | Reconnect attempts before bypass |
| `bypass-recovery-interval` | `30` | Seconds in bypass before retrying (`0` = never) |

Per-call values (URL, capture mode, role, call_id, metadata) come from the
command / channel variables above and override these defaults.

## Roadmap

- **v1.x — fork only** (this line): `v1.0` tap (read/write/mixed/stereo) ✅;
  optional capture resampling (default native) — planned, low priority.
- **v2.x — injection & routing**: inject the server's processed audio back into
  a chosen leg (same-leg, then cross-leg / cross-call). Enables real-time
  translation, prompts, agent whisper.
- **v3.x**: pause/resume, stats API, framed media header, auth extensions
  (Bearer/mTLS), multiplexing, optional gRPC transport.

Design and target protocol: [`docs/DESIGN.md`](docs/DESIGN.md).

## License

[MPL 1.1](LICENSE) — the same license as FreeSWITCH.
