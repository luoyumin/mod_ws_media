# Examples

## `echo_server.py`

A minimal, **zero-dependency** WebSocket server (pure Python stdlib, no
`websockets` package) to try `mod_ws_media` end to end without wiring up an ASR
backend. It completes the RFC 6455 handshake, prints the JSON `start` frame the
module sends, and writes the streamed L16 PCM to a playable WAV on hangup.

```bash
python3 example/echo_server.py [port]     # default 8080
```

Point the module at it:

```bash
uuid_ws_media <uuid> start ws://127.0.0.1:8080/media in=stereo role=agent
```

Recordings are written to `./ws_media_recordings/`. For `stereo` capture the WAV
has left = read (self), right = write (peer).

> Illustrative only — a minimal reference sink, not production code.
