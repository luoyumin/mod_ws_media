#!/usr/bin/env python3
"""
Test WebSocket server for mod_ws_media
This is a simple echo server that receives audio and sends it back.
"""

import asyncio
import websockets
import base64
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Statistics
stats = {
    'connections': 0,
    'bytes_received': 0,
    'bytes_sent': 0,
    'frames_received': 0,
    'frames_sent': 0
}

# Store session info
sessions = {}


async def handle_client(websocket, path):
    """Handle a WebSocket client connection"""
    client_id = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
    stats['connections'] += 1
    session_info = {}

    logger.info(f"[{client_id}] New connection (path: {path})")
    logger.info(f"[{client_id}] Total connections: {stats['connections']}")

    # Check for authentication header
    auth_header = websocket.request_headers.get('Authorization')
    if auth_header:
        logger.info(f"[{client_id}] Authentication header: {auth_header}")
        # Parse Basic Auth
        if auth_header.startswith('Basic '):
            try:
                encoded = auth_header[6:]
                decoded = base64.b64decode(encoded).decode('utf-8')
                username, password = decoded.split(':', 1)
                logger.info(f"[{client_id}] Username: {username}, Password: {'*' * len(password)}")
            except Exception as e:
                logger.error(f"[{client_id}] Failed to parse auth header: {e}")

    # Log query parameters
    if '?' in path:
        query_string = path.split('?', 1)[1]
        logger.info(f"[{client_id}] Query parameters: {query_string}")

    try:
        async for message in websocket:
            if isinstance(message, bytes):
                # Binary data (audio)
                stats['bytes_received'] += len(message)
                stats['frames_received'] += 1

                logger.debug(f"[{client_id}] Received {len(message)} bytes of audio data")

                # Echo the audio back
                # In a real application, you would process the audio here
                # For example: translate, denoise, or analyze

                await websocket.send(message)

                stats['bytes_sent'] += len(message)
                stats['frames_sent'] += 1

                # Log statistics every 100 frames
                if stats['frames_received'] % 100 == 0:
                    logger.info(f"[{client_id}] Stats: "
                              f"RX: {stats['frames_received']} frames ({stats['bytes_received']} bytes), "
                              f"TX: {stats['frames_sent']} frames ({stats['bytes_sent']} bytes)")
            else:
                # Text message - check if it's an init packet
                logger.info(f"[{client_id}] Received text message: {message}")

                try:
                    data = json.loads(message)
                    if data.get('type') == 'init':
                        # Store session initialization info
                        session_info = {
                            'uuid': data.get('uuid'),
                            'direction': data.get('direction'),
                            'encoding': data.get('encoding', 'L16'),
                            'sample_rate': data.get('sample_rate'),
                            'channels': data.get('channels', 1),
                            'ptime': data.get('ptime'),
                            'bytes_per_frame': data.get('bytes_per_frame'),
                            'channel_codec': data.get('channel_codec'),
                        }
                        sessions[client_id] = session_info

                        logger.info(f"[{client_id}] ═══════════════════════════════════════")
                        logger.info(f"[{client_id}] Session initialized:")
                        logger.info(f"[{client_id}]   UUID:           {session_info['uuid']}")
                        logger.info(f"[{client_id}]   Direction:      {session_info['direction']}")
                        logger.info(f"[{client_id}]   Encoding:       {session_info['encoding']}")
                        logger.info(f"[{client_id}]   Sample Rate:    {session_info['sample_rate']} Hz")
                        logger.info(f"[{client_id}]   Channels:       {session_info['channels']}")
                        logger.info(f"[{client_id}]   Ptime:          {session_info['ptime']} ms")
                        logger.info(f"[{client_id}]   Bytes/Frame:    {session_info['bytes_per_frame']}")
                        logger.info(f"[{client_id}]   Channel Codec:  {session_info['channel_codec']}")
                        logger.info(f"[{client_id}] ═══════════════════════════════════════")

                        # Send acknowledgment back
                        ack = json.dumps({
                            'type': 'init_ack',
                            'status': 'ok',
                            'uuid': session_info['uuid']
                        })
                        await websocket.send(ack)
                        logger.info(f"[{client_id}] Sent init acknowledgment")
                except json.JSONDecodeError:
                    logger.warning(f"[{client_id}] Received non-JSON text message: {message}")

    except websockets.exceptions.ConnectionClosed as e:
        logger.info(f"[{client_id}] Connection closed: {e}")
    except Exception as e:
        logger.error(f"[{client_id}] Error: {e}", exc_info=True)
    finally:
        if client_id in sessions:
            logger.info(f"[{client_id}] Session UUID: {sessions[client_id]['uuid']}")
            del sessions[client_id]
        logger.info(f"[{client_id}] Connection ended")
        logger.info(f"[{client_id}] Session stats: "
                  f"RX: {stats['frames_received']} frames ({stats['bytes_received']} bytes), "
                  f"TX: {stats['frames_sent']} frames ({stats['bytes_sent']} bytes)")


async def main():
    """Start the WebSocket server"""
    host = '0.0.0.0'
    port = 8080

    logger.info(f"Starting WebSocket server on {host}:{port}")
    logger.info("Press Ctrl+C to stop")

    async with websockets.serve(handle_client, host, port):
        await asyncio.Future()  # Run forever


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
        logger.info(f"Final stats: "
                  f"Connections: {stats['connections']}, "
                  f"RX: {stats['frames_received']} frames ({stats['bytes_received']} bytes), "
                  f"TX: {stats['frames_sent']} frames ({stats['bytes_sent']} bytes)")
