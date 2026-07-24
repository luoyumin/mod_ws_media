#!/usr/bin/env python3
"""
Test WebSocket server for mod_ws_media (v1 tap protocol).

Behaviour:
  - Reads the first text frame as the v1 `start` control message and learns the
    media format (sample_rate / channels), capture mode and per-channel roles.
  - Writes the received binary L16 PCM into WAV file(s) under ./recordings/:
      * mono  (in=read|write|mixed) -> one file
      * stereo(in=stereo)           -> de-interleaved into two mono files
        (left / right), named with the track roles so you can verify the
        speaker<->channel mapping by ear.
  - Prints throughput stats. It does NOT echo audio back (v1 is tap-only).

Compatible with websockets >= 14 (incl. 16.x) and older.
"""

import asyncio
import json
import logging
import os
import time
import wave

import websockets

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

REC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "recordings")


def _safe(name):
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in (name or "x"))


class WavSink:
    """Writes incoming (possibly stereo-interleaved) L16 to one or two mono WAVs."""

    def __init__(self, call_id, sample_rate, channels, mode, tracks):
        os.makedirs(REC_DIR, exist_ok=True)
        self.channels = channels if channels in (1, 2) else 1
        self.rate = sample_rate or 8000
        self.leftover = b""
        ts = time.strftime("%Y%m%d-%H%M%S")
        base = f"{_safe(call_id)}_{ts}"

        def role_of(ch, default):
            for t in (tracks or []):
                if t.get("ch") == ch:
                    return t.get("role", default)
            return default

        if self.channels == 2:
            l = os.path.join(REC_DIR, f"{base}_left-{_safe(role_of(0, 'ch0'))}.wav")
            r = os.path.join(REC_DIR, f"{base}_right-{_safe(role_of(1, 'ch1'))}.wav")
            self.writers = [self._open(l), self._open(r)]
            self.paths = [l, r]
        else:
            p = os.path.join(REC_DIR, f"{base}_{_safe(mode or 'mono')}.wav")
            self.writers = [self._open(p)]
            self.paths = [p]
        logger.info("Recording -> %s", ", ".join(self.paths))

    def _open(self, path):
        w = wave.open(path, "wb")
        w.setnchannels(1)
        w.setsampwidth(2)   # L16
        w.setframerate(self.rate)
        return w

    def write(self, data):
        buf = self.leftover + data
        if self.channels == 2:
            n = len(buf) - (len(buf) % 4)          # whole L/R sample-pairs (4 bytes)
            frame, self.leftover = buf[:n], buf[n:]
            # de-interleave [L R L R ...]
            left = b"".join(frame[i:i + 2] for i in range(0, n, 4))
            right = b"".join(frame[i + 2:i + 4] for i in range(0, n, 4))
            self.writers[0].writeframes(left)
            self.writers[1].writeframes(right)
        else:
            n = len(buf) - (len(buf) % 2)          # whole samples (2 bytes)
            frame, self.leftover = buf[:n], buf[n:]
            self.writers[0].writeframes(frame)

    def close(self):
        for w in self.writers:
            try:
                w.close()
            except Exception:
                pass


async def handle_client(websocket, path=None):
    request = getattr(websocket, "request", None)
    if path is None:
        path = getattr(request, "path", "") if request is not None else ""
    client = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
    logger.info("[%s] connection open (path: %s)", client, path)

    sink = None
    started = False
    frames = 0
    total_bytes = 0
    try:
        async for message in websocket:
            if isinstance(message, bytes):
                total_bytes += len(message)
                frames += 1
                if sink is None:
                    # binary before start: assume mono 8k
                    logger.warning("[%s] audio before start; defaulting mono/8000", client)
                    sink = WavSink("nostart", 8000, 1, "mono", [])
                sink.write(message)
                if frames % 100 == 0:
                    logger.info("[%s] RX %d frames (%d bytes)", client, frames, total_bytes)
            else:
                logger.info("[%s] text: %s", client, message)
                if started:
                    continue
                try:
                    d = json.loads(message)
                except json.JSONDecodeError:
                    logger.warning("[%s] non-JSON text ignored", client)
                    continue
                if d.get("event") == "start":
                    started = True
                    mf = d.get("media_format", {})
                    cap = d.get("capture", {})
                    logger.info("[%s] START call_id=%s rate=%s ch=%s mode=%s",
                                client, d.get("call_id"), mf.get("sample_rate"),
                                mf.get("channels"), cap.get("mode"))
                    sink = WavSink(d.get("call_id", "call"),
                                   int(mf.get("sample_rate") or 8000),
                                   int(mf.get("channels") or 1),
                                   cap.get("mode"),
                                   cap.get("tracks"))
    except websockets.exceptions.ConnectionClosed as e:
        logger.info("[%s] closed: %s", client, e)
    finally:
        if sink:
            sink.close()
        logger.info("[%s] ended. RX %d frames (%d bytes)", client, frames, total_bytes)


async def main():
    host, port = "0.0.0.0", 8080
    logger.info("WebSocket tap test server on %s:%d  (recordings -> %s)", host, port, REC_DIR)
    logger.info("Ctrl+C to stop")
    async with websockets.serve(handle_client, host, port):
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("stopped")
