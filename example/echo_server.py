#!/usr/bin/env python3
"""
Minimal zero-dependency WebSocket server for trying mod_ws_media locally.

It is the endpoint the module taps into. For every connection it:
  - completes the RFC 6455 handshake (stdlib only, no `websockets` package),
  - prints the JSON `start` frame the module sends first (call_id, capture
    mode, sample rate, channels, roles),
  - collects the binary L16 PCM that follows and, on hangup, writes it to a
    playable WAV (mono, or stereo with left = read/self, right = write/peer).

Run:    python3 example/echo_server.py [port]      # default 8080
Point the module at it, e.g. in the dialplan:
    <action application="ws_media_start"
            data="ws://127.0.0.1:8080/media in=stereo role=agent"/>
or on a live call:
    uuid_ws_media <uuid> start ws://127.0.0.1:8080/media in=stereo role=agent

WAVs land in ./ws_media_recordings/. Pure stdlib — needs only Python 3.7+.
"""
import base64
import hashlib
import json
import os
import socket
import struct
import sys
import threading
import wave

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"  # RFC 6455 magic string
OUT_DIR = "ws_media_recordings"


def handshake(conn):
    """Complete the RFC 6455 upgrade. Return True on success."""
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = conn.recv(4096)
        if not chunk:
            return False
        data += chunk
    key = None
    for line in data.split(b"\r\n"):
        if line.lower().startswith(b"sec-websocket-key:"):
            key = line.split(b":", 1)[1].strip()
    if key is None:
        return False
    accept = base64.b64encode(hashlib.sha1(key + GUID.encode()).digest())
    conn.sendall(
        b"HTTP/1.1 101 Switching Protocols\r\n"
        b"Upgrade: websocket\r\n"
        b"Connection: Upgrade\r\n"
        b"Sec-WebSocket-Accept: " + accept + b"\r\n\r\n"
    )
    return True


def recv_exact(conn, n):
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def read_frame(conn):
    """Read one WebSocket frame. Return (opcode, payload) or (None, None)."""
    hdr = recv_exact(conn, 2)
    if not hdr:
        return None, None
    opcode = hdr[0] & 0x0F
    masked = hdr[1] & 0x80
    length = hdr[1] & 0x7F
    if length == 126:
        length = struct.unpack(">H", recv_exact(conn, 2))[0]
    elif length == 127:
        length = struct.unpack(">Q", recv_exact(conn, 8))[0]
    mask = recv_exact(conn, 4) if masked else b"\x00\x00\x00\x00"
    payload = recv_exact(conn, length) if length else b""
    if payload is None:
        return None, None
    if masked:  # client -> server frames are always masked
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return opcode, payload


def handle(conn, addr):
    if not handshake(conn):
        conn.close()
        return
    rate, channels, call_id = 8000, 1, "call"
    pcm = bytearray()
    saw_stop = False       # got the {"event":"stop"} frame
    clean_close = False    # got a Close frame rather than a bare EOF
    try:
        while True:
            opcode, payload = read_frame(conn)
            if opcode is None:                       # EOF without a Close frame
                break
            if opcode == 0x8:                        # Close -> echo it back
                code = struct.unpack(">H", payload[:2])[0] if len(payload) >= 2 else 1005
                reason = payload[2:].decode("utf-8", "replace")
                print("[close] call_id=%s code=%d%s"
                      % (call_id, code, (" reason=%s" % reason) if reason else ""))
                conn.sendall(b"\x88" + bytes([len(payload)]) + payload)
                clean_close = True
                break
            if opcode == 0x9:                        # Ping -> Pong (unmasked)
                conn.sendall(b"\x8a" + bytes([len(payload)]) + payload)
                continue
            if opcode == 0x1:                        # Text = a JSON control frame
                try:
                    d = json.loads(payload.decode("utf-8"))
                except ValueError:
                    continue
                if d.get("event") == "stop":
                    saw_stop = True
                    print("[stop]  call_id=%s reason=%s"
                          % (d.get("call_id") or call_id, d.get("reason")))
                    continue
                if d.get("event") == "start":
                    mf = d.get("media_format", {})
                    rate = int(mf.get("sample_rate") or 8000)
                    channels = int(mf.get("channels") or 1)
                    call_id = d.get("call_id") or "call"
                    cap = d.get("capture", {})
                    roles = ",".join(t.get("role", "?") for t in cap.get("tracks", []))
                    print("[start] call_id=%s mode=%s %dHz ch=%d roles=[%s]"
                          % (call_id, cap.get("mode"), rate, channels, roles))
                continue
            if opcode in (0x0, 0x2):                 # Binary / continuation = L16 PCM
                pcm += payload
    finally:
        conn.close()
        if pcm:
            os.makedirs(OUT_DIR, exist_ok=True)
            safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in call_id)
            path = os.path.join(OUT_DIR, "%s_%d.wav" % (safe, addr[1]))
            with wave.open(path, "wb") as w:
                w.setnchannels(channels)
                w.setsampwidth(2)                    # L16 = 16-bit
                w.setframerate(rate)
                w.writeframes(bytes(pcm))
            secs = len(pcm) / (2 * channels * rate) if rate else 0
            # A stream that ends without stop+Close was cut off, not finished —
            # anything you accumulated for this call_id is incomplete.
            state = "complete" if (saw_stop and clean_close) else "INCOMPLETE (no stop/close)"
            print("[wav]   call_id=%s wrote %s (%d bytes, %.1fs) — %s"
                  % (call_id, path, len(pcm), secs, state))


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))
    srv.listen(64)
    print("mod_ws_media example server on ws://0.0.0.0:%d/  (Ctrl-C to stop)" % port)
    try:
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=handle, args=(conn, addr), daemon=True).start()
    except KeyboardInterrupt:
        print("\nbye")
    finally:
        srv.close()


if __name__ == "__main__":
    main()
