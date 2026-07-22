# mod_ws_media

A FreeSWITCH module that streams a call's audio to an external service over a
**WebSocket** in real time, and (optionally) injects processed audio back into
the call. The transport is raw **L16 PCM** — the server never has to deal with
the RTP codec. Built for real-time ASR / transcription, translation, recording,
and audio analytics.

The module is a self-contained WebSocket client (RFC 6455) implemented directly
on top of OpenSSL sockets — **no libwebsockets, no gRPC, no extra runtime
dependencies** beyond OpenSSL.

---

> ### ⚠️ Status: pre-release — not production ready
>
> This module builds and runs, but it has **known correctness/robustness bugs**
> (see [Known issues & roadmap](#known-issues--roadmap)) and has **not** been
> validated under production traffic. No tagged release is published yet. Use it
> for evaluation and development only until the issues below are resolved.

---

## Contents

- [Features](#features)
- [How it works](#how-it-works)
- [Requirements](#requirements)
- [Build & install](#build--install)
- [Load the module](#load-the-module)
- [Configuration](#configuration)
- [Usage](#usage)
- [Processing modes](#processing-modes)
- [Events](#events)
- [WebSocket protocol](#websocket-protocol)
- [Use case: real-time ASR with speaker separation](#use-case-real-time-asr-with-speaker-separation)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Known issues & roadmap](#known-issues--roadmap)
- [License](#license)

## Features

- **Two processing modes**
  - **Parallel** — copy the audio out to the WebSocket without touching the
    call (recording, ASR, analytics).
  - **Serial** — replace the call audio with what the server sends back
    (translation, noise reduction, voice conversion).
- **Codec-agnostic** — media-bug audio is always decoded 16-bit signed PCM
  (L16), so the server works the same for PCMU/PCMA/G.722/Opus.
- **Per-direction connections** — the two audio directions (what the channel
  hears vs. what it speaks) are streamed on two independent WebSocket
  connections, which gives you natural speaker separation.
- **Low-latency by design** — small buffers with an overflow-drop policy that
  favors latency over completeness.
- **Graceful degradation** — automatic reconnect, and a *bypass* mode that lets
  the call continue untouched when the backend is unreachable or the drop rate
  is too high.
- **Auth & handshake options** — HTTP Basic auth and custom query parameters on
  the WebSocket handshake.
- **Observability** — fires FreeSWITCH CUSTOM events for start/stop/connect/
  disconnect/error.
- **TLS** — `wss://` via OpenSSL.

## How it works

The module attaches a [media bug](https://developer.signalwire.com/freeswitch/)
to the channel. In the bug callback FreeSWITCH hands over **decoded L16 PCM**
frames, independent of the negotiated RTP codec. Those frames are queued and
sent to the WebSocket server by dedicated worker threads; in serial mode the
frames returned by the server are queued back and written into the call.

Each call opens **two** WebSocket connections:

| Connection | `direction` in init | Captured audio        | Server audio injected into |
|------------|---------------------|-----------------------|----------------------------|
| READ       | `read`              | what the channel hears (far end) | near end (serial mode) |
| WRITE      | `write`             | what the channel speaks (near end) | far end (serial mode) |

## Requirements

- FreeSWITCH **1.10.x** (headers to build against).
- OpenSSL (libssl + libcrypto).
- A C compiler + `make`. `pkg-config` recommended.

The channel must have a **real media path** — the media bug cannot attach to a
channel in `bypass_media`/`proxy_media` mode or one that is only `park`ed. Use
`echo`, `playback`, `bridge`, etc.

## Build & install

### Out-of-tree, against an installed FreeSWITCH (recommended)

If `pkg-config --exists freeswitch` works (i.e. the FreeSWITCH dev headers are
installed), just:

```bash
make
sudo make install          # installs mod_ws_media.so into FreeSWITCH's module dir
                           # and ws_media.conf.xml into autoload_configs (if absent)
```

Point at a specific FreeSWITCH prefix if pkg-config isn't set up:

```bash
make            FREESWITCH_PATH=/usr/local/freeswitch
sudo make install FREESWITCH_PATH=/usr/local/freeswitch
```

On **macOS** with Homebrew OpenSSL, expose it to pkg-config first:

```bash
export PKG_CONFIG_PATH="$(brew --prefix openssl@3)/lib/pkgconfig:$PKG_CONFIG_PATH"
make
```

Check what the build system detected at any time:

```bash
make print-config
```

### In-tree, like the bundled modules (mod_unimrcp, …)

To build it as part of a FreeSWITCH **source** tree — the same way the shipped
modules are built:

```bash
# 1. put the module where FreeSWITCH expects application modules
cp -r mod_ws_media <freeswitch-src>/src/mod/applications/mod_ws_media
#    (or symlink it)

# 2. register it so the build system picks it up
echo "applications/mod_ws_media" >> <freeswitch-src>/modules.conf

# 3. build (whole tree, or just this module)
cd <freeswitch-src>
make
# or:  cd src/mod/applications/mod_ws_media && make && make install
```

`Makefile.am` in this repo is the in-tree build file; the plain `Makefile` is
the out-of-tree one.

## Load the module

```bash
fs_cli -x "load mod_ws_media"
fs_cli -x "module_exists mod_ws_media"     # -> true
```

To autoload on startup, add to `autoload_configs/modules.conf.xml`:

```xml
<load module="mod_ws_media"/>
```

## Configuration

`autoload_configs/ws_media.conf.xml`:

```xml
<configuration name="ws_media.conf" description="WebSocket Media Bridge Module">
  <settings>
    <param name="ws-host" value="localhost"/>
    <param name="ws-port" value="8080"/>
    <param name="ws-path" value="/media"/>
    <param name="ws-ssl" value="false"/>
    <param name="ws-ssl-verify" value="false"/>

    <!-- optional HTTP Basic auth on the handshake -->
    <!-- <param name="ws-auth-user" value="username"/> -->
    <!-- <param name="ws-auth-pass" value="password"/> -->

    <!-- optional query params appended to the handshake URL -->
    <!-- <param name="ws-query-params" value="session_id=123&amp;api_key=abc"/> -->

    <param name="max-queue-size" value="8192"/>
    <param name="drop-threshold" value="4096"/>
    <param name="reconnect-interval" value="5"/>
    <param name="max-retry-count" value="3"/>
    <param name="packet-loss-threshold" value="0.3"/>
  </settings>
</configuration>
```

| Param | Default | Meaning |
|-------|---------|---------|
| `ws-host` | `localhost` | WebSocket server host |
| `ws-port` | `8080` | WebSocket server port |
| `ws-path` | `/media` | WebSocket path |
| `ws-ssl` | `false` | Use TLS (`wss://`) |
| `ws-ssl-verify` | `false` | When TLS is on, verify the server certificate (uses the system trust store). Off logs a warning. |
| `ws-auth-user` / `ws-auth-pass` | – | HTTP Basic auth (optional) |
| `ws-query-params` | – | Extra query string on the handshake URL |
| `max-queue-size` | `8192` | Per-buffer byte cap before dropping |
| `drop-threshold` | `4096` | Target size to trim back to when dropping |
| `reconnect-interval` | `5` | Seconds between reconnect attempts (also used as socket I/O timeout) |
| `max-retry-count` | `3` | Reconnect attempts before switching to bypass |
| `packet-loss-threshold` | `0.3` | Drop-rate (0.0–1.0) above which bypass kicks in |

> All connection settings are currently **global** (module-wide), not per-call.

## Usage

### Dialplan

```xml
<extension name="ws_media_demo">
  <condition field="destination_number" expression="^9999$">
    <action application="answer"/>
    <action application="set" data="ws_media_mode=parallel"/>  <!-- or serial -->
    <action application="ws_media_start"/>
    <action application="bridge" data="user/1000"/>
    <action application="ws_media_stop"/>
  </condition>
</extension>
```

### API / ESL

```bash
fs_cli -x "uuid_setvar <uuid> ws_media_mode parallel"
fs_cli -x "uuid_ws_media <uuid> start"
fs_cli -x "uuid_ws_media <uuid> stop"
```

### originate

```bash
originate {ws_media_mode=parallel,bypass_media=false}user/1000 &echo
```

## Processing modes

Set via the channel variable `ws_media_mode`:

| Value | Mode | Effect |
|-------|------|--------|
| `serial` / `true` (default) | Serial | Call audio is **replaced** by what the server returns |
| `parallel` / `false` | Parallel | Call audio is **copied** out; the original stream is untouched; server audio is ignored |

## Events

CUSTOM events, subclass prefix `ws_media::`:

| Event | Notes |
|-------|-------|
| `ws_media::start` / `ws_media::stop` | Processing lifecycle |
| `ws_media::connected` / `ws_media::disconnected` | Per direction (`Direction: READ|WRITE`) |
| `ws_media::error` | Includes an `Error` header |
| `ws_media::audio_sent` / `ws_media::audio_received` | Audio flow |

```bash
fs_cli> /events plain CUSTOM ws_media::start ws_media::stop ws_media::error
```

## WebSocket protocol

On connect the module sends one JSON **text** frame describing the stream, then
streams **binary** L16 PCM frames. Full wire format, framing rules, control
frames, and a minimal server contract are documented in
[`docs/PROTOCOL.md`](docs/PROTOCOL.md).

```json
{
  "type": "init",
  "uuid": "<channel-uuid>",
  "direction": "read",
  "encoding": "L16",
  "sample_rate": 8000,
  "channels": 1,
  "ptime": 20,
  "bytes_per_frame": 320,
  "channel_codec": "PCMU"
}
```

> A binary WebSocket message is **not** guaranteed to equal one FreeSWITCH audio
> frame — use `bytes_per_frame` for validation, not as a fixed message size.

## Use case: real-time ASR with speaker separation

For live transcription you want **parallel** mode (never modify the call) and
you attach the bug to the **agent leg**:

- the **READ** connection carries the *customer* audio (what the agent hears),
- the **WRITE** connection carries the *agent* audio (what the agent speaks).

Two mono streams, already separated by speaker — feed each to its own streaming
ASR session. Because the payload is L16, the ASR gateway just resamples if
needed (e.g. 8 kHz telephony → 16 kHz) and forwards to the recognizer.

> Verify the READ/WRITE ↔ speaker mapping once on your setup during POC; it
> depends on which leg you attach to.

## Testing

A minimal echo server is included:

```bash
pip3 install websockets
python3 test/test_ws_server.py      # listens on 0.0.0.0:8080, echoes audio, logs init
```

Diagnose a channel that refuses the media bug:

```bash
FS_CLI=/path/to/fs_cli scripts/check_channel.sh <uuid>
```

## Troubleshooting

- **`Failed to add media bug (status=9)`** — the channel has no real media path.
  Avoid `park`; ensure `bypass_media=false` and `proxy_media=false`.
- **Handshake fails** — check host/port/path and that the server speaks RFC 6455;
  raise FreeSWITCH log level with `/log 7`.
- **Choppy injected audio (serial)** — the returned PCM must match the init
  `sample_rate`/`channels` exactly; consider larger `max-queue-size`.

## Known issues & roadmap

This is why there is no release yet. Contributions welcome.

**Fixed on branch `fix/ws_media_p0_bugs` (compiles clean; pending live-call validation):**

- [x] **Serial mode cached replace-frame pointer** — now writes the current
      callback's replace frame in place (`get → memcpy → set`), never cached.
- [x] **No cleanup on media-bug `CLOSE`** — teardown now runs from
      `SWITCH_ABC_TYPE_CLOSE` via a single idempotent `ws_media_cleanup()`, so an
      abnormal hangup (or API start without stop) no longer leaks buffers.
- [x] **Concurrent writes to one socket** — added a per-direction send lock so
      audio frames and Pong/init frames can't interleave and corrupt framing.
- [x] **Blocking connect in `ws_media_start`** — connections are now established
      by the receive threads; call setup is no longer stalled by a slow backend.
      (Also fixed an ordering point so audio can't be sent before the init frame.)
- [x] **TLS hardening** — `TLS_client_method` instead of deprecated
      `SSLv23_client_method`, SNI set for hostnames, and optional server-cert
      verification via `ws-ssl-verify` (default off, logs a warning).

**Still open (must-fix / should-fix before production):**

- [ ] **`bypass` is permanent** — once tripped it never recovers for that call.
- [ ] Statistics counters are updated from multiple threads without atomics, so
      the reported drop rate is unreliable.
- [ ] Config reload frees the old config pool while active calls may still read
      it; treat reload as unsafe during live calls for now.
- [ ] Runtime validation under a real FreeSWITCH + live call (this branch has
      been compile- and link-verified only).

**Enhancements:**

- [ ] Per-call / per-channel-variable connection settings (URL, auth) instead of
      global-only config.
- [ ] Send-only mode that skips the receive threads for pure ASR/recording.
- [ ] `uuid_ws_media <uuid> stats` command.
- [ ] Optional single full-duplex connection instead of two.

## License

[MPL 1.1](LICENSE) — the same license as FreeSWITCH.
