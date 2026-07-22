/*
 * FreeSWITCH Modular Media Switching Software Library / Soft-Switch Application
 * Copyright (C) 2024
 *
 * Version: MPL 1.1
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * mod_ws_media.c -- WebSocket-based Media Processing Module
 *
 * This module intercepts audio streams during calls and sends them to a
 * third-party service via WebSocket. The processed audio is
 * received back and injected into the call (serial mode), or
 * the audio is copied and sent for analysis without modification (parallel mode).
 *
 * Authors:
 *   LUOYUMIN <luoyumin@meiqia.com>          -- original design & implementation
 *   Claude (Anthropic, Opus 4.8) <claude>   -- co-author; wrote the P0/P1
 *       correctness & robustness fixes (replace-frame handling, CLOSE-time
 *       cleanup, per-direction send locks, async connect + init ordering,
 *       TLS hardening). See the git history (Co-Authored-By trailers) for
 *       per-commit attribution.
 */

#include <switch.h>
#include <switch_curl.h>
#include <openssl/sha.h>
#include <openssl/evp.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/ssl.h>
#include <openssl/rand.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>

/* Module definitions */
SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_ws_media_shutdown);
SWITCH_MODULE_LOAD_FUNCTION(mod_ws_media_load);
SWITCH_MODULE_DEFINITION(mod_ws_media, mod_ws_media_load, mod_ws_media_shutdown, NULL);

/* API and Application prototypes */
SWITCH_STANDARD_API(ws_media_api);
SWITCH_STANDARD_APP(ws_media_start_app);
SWITCH_STANDARD_APP(ws_media_stop_app);

/* Event names */
#define WS_MEDIA_EVENT_START "ws_media::start"
#define WS_MEDIA_EVENT_STOP "ws_media::stop"
#define WS_MEDIA_EVENT_CONNECTED "ws_media::connected"
#define WS_MEDIA_EVENT_DISCONNECTED "ws_media::disconnected"
#define WS_MEDIA_EVENT_ERROR "ws_media::error"
#define WS_MEDIA_EVENT_AUDIO_SENT "ws_media::audio_sent"
#define WS_MEDIA_EVENT_AUDIO_RECEIVED "ws_media::audio_received"

#define WS_GUID "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
#define WS_MAX_FRAME_PAYLOAD (1024 * 1024)
#define WS_MASK_CHUNK_SIZE 4096

/* Configuration */
typedef struct {
	char *ws_url;
	char *ws_host;
	int ws_port;
	char *ws_path;
	int ws_ssl;
	int ws_ssl_verify;         /* Verify server certificate when ws_ssl is on */
	char *ws_auth_user;        /* Authentication username */
	char *ws_auth_pass;        /* Authentication password */
	char *ws_query_params;     /* Query parameters for WebSocket handshake */
	int max_queue_size;
	int drop_threshold;
	int reconnect_interval;
	int max_retry_count;       /* Maximum retry attempts before bypass mode */
	float packet_loss_threshold; /* Packet loss rate threshold (0.0-1.0) */
	switch_memory_pool_t *config_pool; /* Pool for config strings — destroyed on reload */
} ws_media_globals_t;

static ws_media_globals_t globals = {0};

/* WebSocket session data */
typedef struct {
	switch_core_session_t *session;
	switch_media_bug_t *bug;
	char *uuid;

	/* Processing mode */
	switch_bool_t serial_mode;  /* TRUE: serial processing (replace), FALSE: parallel (copy) */

	/* WebSocket connections - one for each direction */
	/* READ direction (B -> A) connection */
	int read_ws_socket;
	SSL_CTX *read_ssl_ctx;
	SSL *read_ssl;
	int read_connected;
	int read_retry_count;

	/* WRITE direction (A -> B) connection */
	int write_ws_socket;
	SSL_CTX *write_ssl_ctx;
	SSL *write_ssl;
	int write_connected;
	int write_retry_count;

	/* Audio processing */
	switch_codec_implementation_t read_impl;
	switch_codec_implementation_t write_impl;
	/* Buffers for read direction (B -> A): B's audio to WebSocket, processed audio back to A */
	switch_buffer_t *read_send_buffer;  /* B's audio to send to WebSocket */
	switch_buffer_t *read_recv_buffer;  /* Processed audio from WebSocket to A */
	/* Buffers for write direction (A -> B): A's audio to WebSocket, processed audio back to B */
	switch_buffer_t *write_send_buffer; /* A's audio to send to WebSocket */
	switch_buffer_t *write_recv_buffer; /* Processed audio from WebSocket to B */
	switch_mutex_t *audio_mutex;

	/* Per-direction send locks: serialize all writes on a socket (audio frames
	 * from the send thread vs. Pong/init frames from the recv/connect path) so
	 * WebSocket frames never interleave and corrupt the stream. */
	switch_mutex_t *read_send_lock;
	switch_mutex_t *write_send_lock;

	/* Threading - separate threads for each direction */
	switch_thread_t *read_send_thread;  /* Sends B's audio to WebSocket */
	switch_thread_t *read_recv_thread;  /* Receives processed audio for A */
	switch_thread_t *write_send_thread; /* Sends A's audio to WebSocket */
	switch_thread_t *write_recv_thread; /* Receives processed audio for B */
	switch_bool_t running;

	/* Retry and bypass mode */
	switch_bool_t bypass_mode;  /* When true, audio passes through without WebSocket processing */

	/* Statistics - for packet loss monitoring */
	uint64_t frames_sent;
	uint64_t frames_received;
	uint64_t frames_dropped;
	uint64_t bytes_sent;
	uint64_t bytes_received;
	switch_time_t start_time;
	switch_time_t stop_time;
	switch_time_t last_stats_time;
	uint64_t last_frames_sent;
	uint64_t last_frames_dropped;
	float current_packet_loss_rate;

	/* Teardown guard: cleanup runs exactly once, whether triggered by
	 * ws_media_stop or by the media-bug CLOSE callback on hangup. */
	switch_bool_t cleaned_up;
} ws_media_session_t;

/* WebSocket frame structure */
typedef struct {
	uint8_t fin;
	uint8_t opcode;
	uint8_t mask;
	uint64_t payload_len;
	char masking_key[4];
	char *payload;
} ws_frame_t;

/* Forward declarations */
static switch_bool_t ws_media_callback(switch_media_bug_t *bug, void *user_data, switch_abc_type_t type);
static void *SWITCH_THREAD_FUNC read_send_thread(switch_thread_t *thread, void *obj);
static void *SWITCH_THREAD_FUNC read_recv_thread(switch_thread_t *thread, void *obj);
static void *SWITCH_THREAD_FUNC write_send_thread(switch_thread_t *thread, void *obj);
static void *SWITCH_THREAD_FUNC write_recv_thread(switch_thread_t *thread, void *obj);
static switch_status_t ws_connect_read(ws_media_session_t *session);
static switch_status_t ws_connect_write(ws_media_session_t *session);
static void ws_disconnect_read(ws_media_session_t *session);
static void ws_disconnect_write(ws_media_session_t *session);
static switch_status_t ws_handshake(ws_media_session_t *session, switch_bool_t is_read_direction);
static switch_status_t ws_send_init_packet(ws_media_session_t *session, switch_bool_t is_read_direction);
static switch_status_t ws_send_frame(ws_media_session_t *session, const char *data, size_t len, switch_bool_t is_read_direction, switch_bool_t is_text);
static switch_status_t ws_send_frame_opcode(ws_media_session_t *session, const char *data, size_t len, switch_bool_t is_read_direction, uint8_t opcode);
static switch_status_t ws_recv_frame(ws_media_session_t *session, char **data, size_t *len, switch_bool_t is_read_direction, uint8_t *opcode_out);
static char *base64_encode(const unsigned char *input, int length);
static void ws_media_fire_event(const char *event_name, ws_media_session_t *session, const char *key, const char *value);
static void update_packet_loss_stats(ws_media_session_t *session);
static switch_status_t ws_open_socket(int *ws_socket, const char *direction);
static void ws_media_cleanup(ws_media_session_t *session);

/* Reliable full-read over a TCP/SSL stream.
 * TCP is a byte-stream protocol — a single recv() may return fewer bytes
 * than requested.  This helper loops until all `need` bytes are received,
 * the connection is closed, or an error occurs. */
static int recv_exact(int sock, SSL *ssl, void *buf, int need)
{
	int total = 0;
	if (need <= 0) {
		return 0;
	}

	while (total < need) {
		int r;
		if (ssl) {
			r = SSL_read(ssl, (char *)buf + total, need - total);
			if (r <= 0) {
				int ssl_err = SSL_get_error(ssl, r);
				if (ssl_err == SSL_ERROR_WANT_READ || ssl_err == SSL_ERROR_WANT_WRITE) {
					return total == 0 ? -2 : -1;
				}
				return -1;
			}
		} else {
			r = recv(sock, (char *)buf + total, need - total, 0);
			if (r < 0 && errno == EINTR) {
				continue;
			}
			if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
				return total == 0 ? -2 : -1;
			}
			if (r <= 0) return -1;
		}
		total += r;
	}
	return total;
}

/* Reliable full-write over a TCP/SSL stream.
 * A single send()/SSL_write() may transmit fewer bytes than requested. */
static int send_exact(int sock, SSL *ssl, const void *buf, int need)
{
	int total = 0;
	if (need <= 0) {
		return 0;
	}

	while (total < need) {
		int r;
		if (ssl) {
			r = SSL_write(ssl, (const char *)buf + total, need - total);
			if (r <= 0) {
				int ssl_err = SSL_get_error(ssl, r);
				if (ssl_err == SSL_ERROR_WANT_READ || ssl_err == SSL_ERROR_WANT_WRITE) {
					return -1;
				}
				return -1;
			}
		} else {
#ifdef MSG_NOSIGNAL
			r = send(sock, (const char *)buf + total, need - total, MSG_NOSIGNAL);
#else
			r = send(sock, (const char *)buf + total, need - total, 0);
#endif
			if (r < 0 && errno == EINTR) {
				continue;
			}
			if (r <= 0) return -1;
		}
		total += r;
	}
	return total;
}

static int ws_read_some(int sock, SSL *ssl, void *buf, int len)
{
	int r;

	if (ssl) {
		r = SSL_read(ssl, buf, len);
		if (r <= 0) {
			int ssl_err = SSL_get_error(ssl, r);
			if (ssl_err == SSL_ERROR_WANT_READ || ssl_err == SSL_ERROR_WANT_WRITE) {
				return 0;
			}
			return -1;
		}
		return r;
	}

	do {
		r = recv(sock, buf, len, 0);
	} while (r < 0 && errno == EINTR);

	return r;
}

static int ws_io_timeout_sec(void)
{
	return globals.reconnect_interval > 0 ? globals.reconnect_interval : 5;
}

static void ws_set_socket_options(int sock)
{
	struct timeval tv;
	int one = 1;

	tv.tv_sec = ws_io_timeout_sec();
	tv.tv_usec = 0;

	setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

#ifdef TCP_NODELAY
	setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
#endif

#ifdef SO_NOSIGPIPE
	setsockopt(sock, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
}

static int ws_connect_with_timeout(int sock, const struct sockaddr *addr, socklen_t addrlen, int timeout_sec)
{
	int flags;
	int ret;

	flags = fcntl(sock, F_GETFL, 0);
	if (flags < 0) {
		return connect(sock, addr, addrlen);
	}

	if (fcntl(sock, F_SETFL, flags | O_NONBLOCK) < 0) {
		return connect(sock, addr, addrlen);
	}

	ret = connect(sock, addr, addrlen);
	if (ret == 0) {
		fcntl(sock, F_SETFL, flags);
		return 0;
	}

	if (errno == EINPROGRESS) {
		fd_set wfds;
		struct timeval tv;

		FD_ZERO(&wfds);
		FD_SET(sock, &wfds);
		tv.tv_sec = timeout_sec;
		tv.tv_usec = 0;

		do {
			ret = select(sock + 1, NULL, &wfds, NULL, &tv);
		} while (ret < 0 && errno == EINTR);

		if (ret > 0) {
			int so_error = 0;
			socklen_t len = sizeof(so_error);

			if (getsockopt(sock, SOL_SOCKET, SO_ERROR, &so_error, &len) == 0 && so_error == 0) {
				fcntl(sock, F_SETFL, flags);
				return 0;
			}

			errno = so_error ? so_error : errno;
		} else if (ret == 0) {
			errno = ETIMEDOUT;
		}
	}

	fcntl(sock, F_SETFL, flags);
	return -1;
}

static switch_status_t ws_open_socket(int *ws_socket, const char *direction)
{
	struct addrinfo hints;
	struct addrinfo *result = NULL;
	struct addrinfo *rp;
	char port[16];
	int gai_status;

	*ws_socket = -1;
	snprintf(port, sizeof(port), "%d", globals.ws_port);

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = IPPROTO_TCP;

	gai_status = getaddrinfo(globals.ws_host, port, &hints, &result);
	if (gai_status != 0) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
			"Failed to resolve hostname for %s: %s (%s)\n",
			direction, globals.ws_host, gai_strerror(gai_status));
		return SWITCH_STATUS_FALSE;
	}

	for (rp = result; rp; rp = rp->ai_next) {
		int sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);

		if (sock < 0) {
			continue;
		}

		ws_set_socket_options(sock);

		if (ws_connect_with_timeout(sock, rp->ai_addr, (socklen_t)rp->ai_addrlen, ws_io_timeout_sec()) == 0) {
			*ws_socket = sock;
			freeaddrinfo(result);
			return SWITCH_STATUS_SUCCESS;
		}

		close(sock);
	}

	freeaddrinfo(result);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
		"Failed to connect %s to %s:%d\n", direction, globals.ws_host, globals.ws_port);
	return SWITCH_STATUS_FALSE;
}

/* Base64 encoding for WebSocket handshake */
static char *base64_encode(const unsigned char *input, int length)
{
	BIO *bmem, *b64;
	BUF_MEM *bptr;
	char *buff;

	b64 = BIO_new(BIO_f_base64());
	if (!b64) {
		return NULL;
	}
	bmem = BIO_new(BIO_s_mem());
	if (!bmem) {
		BIO_free(b64);
		return NULL;
	}
	b64 = BIO_push(b64, bmem);
	BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
	if (BIO_write(b64, input, length) <= 0 || BIO_flush(b64) != 1) {
		BIO_free_all(b64);
		return NULL;
	}
	BIO_get_mem_ptr(b64, &bptr);
	if (!bptr) {
		BIO_free_all(b64);
		return NULL;
	}

	buff = (char *)malloc(bptr->length + 1);
	if (!buff) {
		BIO_free_all(b64);
		return NULL;
	}
	memcpy(buff, bptr->data, bptr->length);
	buff[bptr->length] = 0;

	BIO_free_all(b64);
	return buff;
}

/* Generate WebSocket key for handshake */
static char *generate_ws_key(void)
{
	unsigned char key[16];

	if (RAND_bytes(key, sizeof(key)) != 1) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to generate WebSocket key\n");
		return NULL;
	}

	return base64_encode(key, sizeof(key));
}

static switch_bool_t ws_validate_handshake_response(const char *response, const char *encoded_key)
{
	char accept_src[256];
	unsigned char sha[SHA_DIGEST_LENGTH];
	char *expected_accept = NULL;
	const char *status_end;
	const char *status_code;
	const char *accept_header;
	const char *accept_value;
	const char *accept_end;
	size_t accept_len;
	switch_bool_t valid = SWITCH_FALSE;

	status_end = strstr(response, "\r\n");
	status_code = strstr(response, " 101 ");
	if (!status_end || strncmp(response, "HTTP/", 5) || !status_code || status_code > status_end) {
		return SWITCH_FALSE;
	}

	if (!switch_stristr("upgrade:", response) || !switch_stristr("websocket", response) ||
		!switch_stristr("connection:", response) || !switch_stristr("upgrade", response)) {
		return SWITCH_FALSE;
	}

	snprintf(accept_src, sizeof(accept_src), "%s%s", encoded_key, WS_GUID);
	SHA1((unsigned char *)accept_src, strlen(accept_src), sha);
	expected_accept = base64_encode(sha, SHA_DIGEST_LENGTH);
	if (!expected_accept) {
		return SWITCH_FALSE;
	}

	accept_header = switch_stristr("sec-websocket-accept:", response);
	if (!accept_header) {
		free(expected_accept);
		return SWITCH_FALSE;
	}

	accept_value = strchr(accept_header, ':');
	if (!accept_value) {
		free(expected_accept);
		return SWITCH_FALSE;
	}
	accept_value++;
	while (*accept_value == ' ' || *accept_value == '\t') {
		accept_value++;
	}

	accept_end = strpbrk(accept_value, "\r\n");
	if (!accept_end) {
		free(expected_accept);
		return SWITCH_FALSE;
	}

	accept_len = (size_t)(accept_end - accept_value);
	while (accept_len > 0 && (accept_value[accept_len - 1] == ' ' || accept_value[accept_len - 1] == '\t')) {
		accept_len--;
	}

	if (strlen(expected_accept) == accept_len && !strncmp(accept_value, expected_accept, accept_len)) {
		valid = SWITCH_TRUE;
	}

	free(expected_accept);
	return valid;
}

/* WebSocket handshake - supports both directions */
static switch_status_t ws_handshake(ws_media_session_t *session, switch_bool_t is_read_direction)
{
	char *encoded_key;
	char handshake[4096];
	char response[4096];
	char auth_header[512];
	char ws_path_with_params[1024];
	int *ws_socket = is_read_direction ? &session->read_ws_socket : &session->write_ws_socket;
	SSL **ssl = is_read_direction ? &session->read_ssl : &session->write_ssl;

	encoded_key = generate_ws_key();
	auth_header[0] = '\0';

	if (!encoded_key) {
		return SWITCH_STATUS_FALSE;
	}

	/* Generate Basic Auth header if credentials are provided */
	if (!zstr(globals.ws_auth_user) && !zstr(globals.ws_auth_pass)) {
		char auth_str[256];
		char *encoded_auth;
		snprintf(auth_str, sizeof(auth_str), "%s:%s", globals.ws_auth_user, globals.ws_auth_pass);
		encoded_auth = base64_encode((unsigned char *)auth_str, strlen(auth_str));
		if (encoded_auth) {
			snprintf(auth_header, sizeof(auth_header), "Authorization: Basic %s\r\n", encoded_auth);
			free(encoded_auth);
		}
	}

	/* Build WebSocket path with query parameters */
	if (!zstr(globals.ws_query_params)) {
		snprintf(ws_path_with_params, sizeof(ws_path_with_params), "%s?%s",
			globals.ws_path ? globals.ws_path : "/", globals.ws_query_params);
	} else {
		snprintf(ws_path_with_params, sizeof(ws_path_with_params), "%s",
			globals.ws_path ? globals.ws_path : "/");
	}

	snprintf(handshake, sizeof(handshake),
		"GET %s HTTP/1.1\r\n"
		"Host: %s:%d\r\n"
		"Upgrade: websocket\r\n"
		"Connection: Upgrade\r\n"
		"Sec-WebSocket-Key: %s\r\n"
		"Sec-WebSocket-Version: 13\r\n"
		"%s"  /* Auth header (if any) */
		"\r\n",
		ws_path_with_params,
		globals.ws_host,
		globals.ws_port,
		encoded_key,
		auth_header);

	/* Send handshake */
	if (send_exact(*ws_socket, *ssl, handshake, strlen(handshake)) < 0) {
		free(encoded_key);
		return SWITCH_STATUS_FALSE;
	}

	/* Read until the complete HTTP header arrives. */
	memset(response, 0, sizeof(response));
	{
		int total = 0;

		while (total < (int)sizeof(response) - 1) {
			int rlen = ws_read_some(*ws_socket, *ssl, response + total, (int)sizeof(response) - 1 - total);
			if (rlen <= 0) {
				free(encoded_key);
				return SWITCH_STATUS_FALSE;
			}
			total += rlen;
			response[total] = '\0';

			if (strstr(response, "\r\n\r\n")) {
				break;
			}
		}

		if (!strstr(response, "\r\n\r\n")) {
			free(encoded_key);
			return SWITCH_STATUS_FALSE;
		}
	}

	if (!ws_validate_handshake_response(response, encoded_key)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "WebSocket handshake failed: %s\n", response);
		free(encoded_key);
		return SWITCH_STATUS_FALSE;
	}

	free(encoded_key);
	return SWITCH_STATUS_SUCCESS;
}

/* Send initialization packet with media parameters to WebSocket server.
 *
 * The audio data exchanged over WebSocket is ALWAYS raw signed 16-bit PCM (L16),
 * regardless of the channel's RTP codec (PCMU, PCMA, Opus, G.711, etc.).
 * FreeSWITCH decodes the RTP payload before delivering frames to the media bug,
 * so the server never needs to handle the compressed codec format.
 *
 * Init packet fields:
 *   encoding        - always "L16" (signed 16-bit little-endian PCM)
 *   sample_rate     - decoded PCM sample rate (actual_samples_per_second)
 *   channels        - number of audio channels (1 = mono, 2 = stereo)
 *   ptime           - packet time in milliseconds
 *   bytes_per_frame - exact byte count per audio frame (decoded_bytes_per_packet)
 *   channel_codec   - the SIP/RTP codec name for informational purposes only
 */
static switch_status_t ws_send_init_packet(ws_media_session_t *session, switch_bool_t is_read_direction)
{
	char init_packet[1024];
	int len;
	switch_codec_implementation_t *impl;

	/* Get codec implementation for this direction */
	impl = is_read_direction ? &session->read_impl : &session->write_impl;

	/* Build JSON initialization packet.
	 * Use actual_samples_per_second (not samples_per_second) so that codecs like
	 * G.722 report their real 16kHz decode rate rather than the 8kHz RTP clock. */
	len = snprintf(init_packet, sizeof(init_packet),
		"{"
		"\"type\":\"init\","
		"\"uuid\":\"%s\","
		"\"direction\":\"%s\","
		"\"encoding\":\"L16\","
		"\"sample_rate\":%u,"
		"\"channels\":%u,"
		"\"ptime\":%u,"
		"\"bytes_per_frame\":%u,"
		"\"channel_codec\":\"%s\""
		"}",
		session->uuid,
		is_read_direction ? "read" : "write",
		impl->actual_samples_per_second,
		(unsigned int)impl->number_of_channels,
		(unsigned int)(impl->microseconds_per_packet / 1000),
		impl->decoded_bytes_per_packet,
		impl->iananame);

	if (len < 0 || len >= (int)sizeof(init_packet)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
			"Failed to build init packet for %s direction\n",
			is_read_direction ? "READ" : "WRITE");
		return SWITCH_STATUS_FALSE;
	}

	/* Send the init packet as a WebSocket text frame */
	if (ws_send_frame(session, init_packet, len, is_read_direction, SWITCH_TRUE) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
			"Failed to send init packet for %s direction\n",
			is_read_direction ? "READ" : "WRITE");
		return SWITCH_STATUS_FALSE;
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE,
		"Sent init packet for %s direction: uuid=%s, encoding=L16, sample_rate=%u, "
		"channels=%u, ptime=%ums, bytes_per_frame=%u, channel_codec=%s\n",
		is_read_direction ? "READ" : "WRITE",
		session->uuid,
		impl->actual_samples_per_second,
		(unsigned int)impl->number_of_channels,
		(unsigned int)(impl->microseconds_per_packet / 1000),
		impl->decoded_bytes_per_packet,
		impl->iananame);

	return SWITCH_STATUS_SUCCESS;
}

/* A bare IP literal is not a valid SNI host name; detect it so we can skip SNI. */
static switch_bool_t host_is_ip_literal(const char *h)
{
	if (zstr(h)) {
		return SWITCH_FALSE;
	}
	if (strchr(h, ':')) {
		return SWITCH_TRUE; /* IPv6 */
	}
	return (strspn(h, "0123456789.") == strlen(h)) ? SWITCH_TRUE : SWITCH_FALSE;
}

/* Establish a TLS session on an already-connected socket.
 * Uses TLS_client_method (SSLv23_* is deprecated), sets SNI for hostnames, and
 * optionally verifies the server certificate (ws-ssl-verify). On any failure
 * the SSL/SSL_CTX are freed and the socket is left for the caller to close. */
static switch_status_t ws_tls_establish(int sock, const char *direction, SSL_CTX **out_ctx, SSL **out_ssl)
{
	SSL_CTX *ctx;
	SSL *ssl;

	*out_ctx = NULL;
	*out_ssl = NULL;

	ctx = SSL_CTX_new(TLS_client_method());
	if (!ctx) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "SSL_CTX_new failed for %s\n", direction);
		return SWITCH_STATUS_FALSE;
	}

	if (globals.ws_ssl_verify) {
		SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, NULL);
		SSL_CTX_set_default_verify_paths(ctx);
	}

	if (!(ssl = SSL_new(ctx))) {
		SSL_CTX_free(ctx);
		return SWITCH_STATUS_FALSE;
	}

	SSL_set_fd(ssl, sock);

	if (!host_is_ip_literal(globals.ws_host)) {
		SSL_set_tlsext_host_name(ssl, globals.ws_host);
	}

	if (SSL_connect(ssl) != 1) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "TLS handshake failed for %s\n", direction);
		SSL_free(ssl);
		SSL_CTX_free(ctx);
		return SWITCH_STATUS_FALSE;
	}

	if (globals.ws_ssl_verify) {
		long vr = SSL_get_verify_result(ssl);
		if (vr != X509_V_OK) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
				"TLS certificate verification failed for %s (result=%ld)\n", direction, vr);
			SSL_free(ssl);
			SSL_CTX_free(ctx);
			return SWITCH_STATUS_FALSE;
		}
	} else {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
			"TLS server certificate NOT verified for %s (set ws-ssl-verify=true to enforce)\n", direction);
	}

	*out_ctx = ctx;
	*out_ssl = ssl;
	return SWITCH_STATUS_SUCCESS;
}

/* Connect to WebSocket server for READ direction (B -> A) */
static switch_status_t ws_connect_read(ws_media_session_t *session)
{
	if (ws_open_socket(&session->read_ws_socket, "READ") != SWITCH_STATUS_SUCCESS) {
		return SWITCH_STATUS_FALSE;
	}
	
	/* TLS if needed */
	if (globals.ws_ssl) {
		if (ws_tls_establish(session->read_ws_socket, "READ", &session->read_ssl_ctx, &session->read_ssl) != SWITCH_STATUS_SUCCESS) {
			close(session->read_ws_socket);
			session->read_ws_socket = -1;
			return SWITCH_STATUS_FALSE;
		}
	}
	
	/* Perform WebSocket handshake */
	if (ws_handshake(session, SWITCH_TRUE) != SWITCH_STATUS_SUCCESS) {
		ws_disconnect_read(session);
		return SWITCH_STATUS_FALSE;
	}

	/* Send the init frame BEFORE marking the direction ready, so the send
	 * thread (which gates on read_connected) can never emit an audio frame
	 * ahead of the init text frame. Low-level sends only require a valid
	 * socket, so the init send below works while read_connected is still 0. */
	if (ws_send_init_packet(session, SWITCH_TRUE) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
			"Failed to send init packet for READ, but connection established\n");
		/* Continue anyway - server might not require init packet */
	}

	session->read_connected = 1;

	ws_media_fire_event(WS_MEDIA_EVENT_CONNECTED, session, "Direction", "READ");

	return SWITCH_STATUS_SUCCESS;
}

/* Connect to WebSocket server for WRITE direction (A -> B) */
static switch_status_t ws_connect_write(ws_media_session_t *session)
{
	if (ws_open_socket(&session->write_ws_socket, "WRITE") != SWITCH_STATUS_SUCCESS) {
		return SWITCH_STATUS_FALSE;
	}
	
	/* TLS if needed */
	if (globals.ws_ssl) {
		if (ws_tls_establish(session->write_ws_socket, "WRITE", &session->write_ssl_ctx, &session->write_ssl) != SWITCH_STATUS_SUCCESS) {
			close(session->write_ws_socket);
			session->write_ws_socket = -1;
			return SWITCH_STATUS_FALSE;
		}
	}
	
	/* Perform WebSocket handshake */
	if (ws_handshake(session, SWITCH_FALSE) != SWITCH_STATUS_SUCCESS) {
		ws_disconnect_write(session);
		return SWITCH_STATUS_FALSE;
	}

	/* Send init BEFORE marking ready (see ws_connect_read for rationale). */
	if (ws_send_init_packet(session, SWITCH_FALSE) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
			"Failed to send init packet for WRITE, but connection established\n");
		/* Continue anyway - server might not require init packet */
	}

	session->write_connected = 1;

	ws_media_fire_event(WS_MEDIA_EVENT_CONNECTED, session, "Direction", "WRITE");

	return SWITCH_STATUS_SUCCESS;
}

/* Disconnect WebSocket for READ direction */
static void ws_disconnect_read(ws_media_session_t *session)
{
	session->read_connected = 0;

	if (session->read_ws_socket >= 0) {
		shutdown(session->read_ws_socket, SHUT_RDWR);
	}

	if (session->read_ssl) {
		SSL_shutdown(session->read_ssl);
		SSL_free(session->read_ssl);
		session->read_ssl = NULL;
	}
	
	if (session->read_ssl_ctx) {
		SSL_CTX_free(session->read_ssl_ctx);
		session->read_ssl_ctx = NULL;
	}
	
	if (session->read_ws_socket >= 0) {
		close(session->read_ws_socket);
		session->read_ws_socket = -1;
	}
	
	ws_media_fire_event(WS_MEDIA_EVENT_DISCONNECTED, session, "Direction", "READ");
}

/* Disconnect WebSocket for WRITE direction */
static void ws_disconnect_write(ws_media_session_t *session)
{
	session->write_connected = 0;

	if (session->write_ws_socket >= 0) {
		shutdown(session->write_ws_socket, SHUT_RDWR);
	}

	if (session->write_ssl) {
		SSL_shutdown(session->write_ssl);
		SSL_free(session->write_ssl);
		session->write_ssl = NULL;
	}
	
	if (session->write_ssl_ctx) {
		SSL_CTX_free(session->write_ssl_ctx);
		session->write_ssl_ctx = NULL;
	}
	
	if (session->write_ws_socket >= 0) {
		close(session->write_ws_socket);
		session->write_ws_socket = -1;
	}
	
	ws_media_fire_event(WS_MEDIA_EVENT_DISCONNECTED, session, "Direction", "WRITE");
}

static void ws_signal_disconnect_read(ws_media_session_t *session)
{
	session->read_connected = 0;
	if (session->read_ws_socket >= 0) {
		shutdown(session->read_ws_socket, SHUT_RDWR);
	}
}

static void ws_signal_disconnect_write(ws_media_session_t *session)
{
	session->write_connected = 0;
	if (session->write_ws_socket >= 0) {
		shutdown(session->write_ws_socket, SHUT_RDWR);
	}
}

/* Send WebSocket frame - RFC 6455 compliant with masking (required for client-to-server frames)
 * is_text: SWITCH_TRUE for text frames (JSON), SWITCH_FALSE for binary frames (audio) */
static switch_status_t ws_send_frame(ws_media_session_t *session, const char *data, size_t len, switch_bool_t is_read_direction, switch_bool_t is_text)
{
	return ws_send_frame_opcode(session, data, len, is_read_direction, is_text ? 0x1 : 0x2);
}

static switch_status_t ws_send_frame_opcode(ws_media_session_t *session, const char *data, size_t len, switch_bool_t is_read_direction, uint8_t opcode)
{
	/* header: 2 base + 8 extended len + 4 masking key = 14 bytes max */
	unsigned char frame[14];
	unsigned char masking_key[4];
	unsigned char chunk[WS_MASK_CHUNK_SIZE];
	size_t header_len;
	size_t offset;
	int *ws_socket = is_read_direction ? &session->read_ws_socket : &session->write_ws_socket;
	SSL **ssl = is_read_direction ? &session->read_ssl : &session->write_ssl;
	int *connected = is_read_direction ? &session->read_connected : &session->write_connected;
	switch_mutex_t *send_lock = is_read_direction ? session->read_send_lock : session->write_send_lock;

	if (len > 0 && !data) {
		return SWITCH_STATUS_FALSE;
	}

	if (opcode >= 0x8 && len > 125) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Invalid WebSocket control frame length: %" SWITCH_SIZE_T_FMT "\n", len);
		return SWITCH_STATUS_FALSE;
	}

	/* Only require a live socket, NOT *connected: the init frame is sent from
	 * ws_connect_* before the direction is marked connected. Audio senders gate
	 * on *_connected separately. */
	if (*ws_socket < 0) {
		return SWITCH_STATUS_FALSE;
	}
	(void)connected;

	/* FIN=1, caller-provided opcode */
	frame[0] = 0x80 | (opcode & 0x0F);

	/* RFC 6455 §5.1: client MUST mask all frames; set MASK bit (0x80) */
	if (RAND_bytes(masking_key, sizeof(masking_key)) != 1) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to generate WebSocket masking key\n");
		return SWITCH_STATUS_FALSE;
	}

	/* Payload length + MASK bit */
	if (len < 126) {
		frame[1] = 0x80 | (unsigned char)len;
		frame[2] = masking_key[0];
		frame[3] = masking_key[1];
		frame[4] = masking_key[2];
		frame[5] = masking_key[3];
		header_len = 6;
	} else if (len < 65536) {
		frame[1] = 0x80 | 126;
		frame[2] = (len >> 8) & 0xFF;
		frame[3] = len & 0xFF;
		frame[4] = masking_key[0];
		frame[5] = masking_key[1];
		frame[6] = masking_key[2];
		frame[7] = masking_key[3];
		header_len = 8;
	} else {
		uint64_t payload_len = (uint64_t)len;

		frame[1] = 0x80 | 127;
		frame[2] = (payload_len >> 56) & 0xFF;
		frame[3] = (payload_len >> 48) & 0xFF;
		frame[4] = (payload_len >> 40) & 0xFF;
		frame[5] = (payload_len >> 32) & 0xFF;
		frame[6] = (payload_len >> 24) & 0xFF;
		frame[7] = (payload_len >> 16) & 0xFF;
		frame[8] = (payload_len >> 8) & 0xFF;
		frame[9] = payload_len & 0xFF;
		frame[10] = masking_key[0];
		frame[11] = masking_key[1];
		frame[12] = masking_key[2];
		frame[13] = masking_key[3];
		header_len = 14;
	}

	/* Serialize the whole header+payload write so a concurrent send on the same
	 * socket (audio from the send thread vs. Pong/init from the recv/connect
	 * path) cannot interleave and corrupt WebSocket framing. */
	if (send_lock) {
		switch_mutex_lock(send_lock);
	}

	/* Send frame header */
	if (send_exact(*ws_socket, *ssl, frame, header_len) < 0) {
		if (send_lock) switch_mutex_unlock(send_lock);
		return SWITCH_STATUS_FALSE;
	}

	/* Mask and send payload in bounded stack chunks to avoid per-frame malloc/free. */
	for (offset = 0; offset < len; ) {
		size_t chunk_len = len - offset;
		size_t i;

		if (chunk_len > sizeof(chunk)) {
			chunk_len = sizeof(chunk);
		}

		for (i = 0; i < chunk_len; i++) {
			chunk[i] = ((const unsigned char *)data)[offset + i] ^ masking_key[(offset + i) % 4];
		}

		if (send_exact(*ws_socket, *ssl, chunk, (int)chunk_len) < 0) {
			if (send_lock) switch_mutex_unlock(send_lock);
			return SWITCH_STATUS_FALSE;
		}

		offset += chunk_len;
	}

	if (send_lock) {
		switch_mutex_unlock(send_lock);
	}

	session->bytes_sent += len;
	return SWITCH_STATUS_SUCCESS;
}

/* Receive WebSocket frame - pure audio data, no direction marker
 * opcode_out: receives the frame opcode (1=text, 2=binary, 8=close, etc.) */
static switch_status_t ws_recv_frame(ws_media_session_t *session, char **data, size_t *len, switch_bool_t is_read_direction, uint8_t *opcode_out)
{
	unsigned char header[14];
	uint64_t payload_len;
	int header_len = 2;
	uint8_t opcode;
	int *ws_socket = is_read_direction ? &session->read_ws_socket : &session->write_ws_socket;
	SSL **ssl = is_read_direction ? &session->read_ssl : &session->write_ssl;
	int *connected = is_read_direction ? &session->read_connected : &session->write_connected;

	if (opcode_out) {
		*opcode_out = 0;
	}
	if (data) {
		*data = NULL;
	}
	if (len) {
		*len = 0;
	}

	if (*ws_socket < 0) {
		return SWITCH_STATUS_FALSE;
	}
	(void)connected;

	/* Read 2-byte base header */
	{
		int r = recv_exact(*ws_socket, *ssl, header, 2);
		if (r == -2) {
			return SWITCH_STATUS_TIMEOUT;
		}
		if (r != 2) {
			return SWITCH_STATUS_FALSE;
		}
	}

	opcode = header[0] & 0x0F;
	if (opcode_out) {
		*opcode_out = opcode;
	}

	if (header[0] & 0x70) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Invalid WebSocket frame: reserved bits set\n");
		return SWITCH_STATUS_FALSE;
	}

	if (!(header[0] & 0x80)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Fragmented WebSocket frames are not supported\n");
		return SWITCH_STATUS_FALSE;
	}

	/* Close frame */
	if (opcode == 0x8) {
		return SWITCH_STATUS_FALSE;
	}

	/* Get payload length */
	payload_len = header[1] & 0x7F;

	if (payload_len == 126) {
		if (recv_exact(*ws_socket, *ssl, &header[2], 2) != 2) return SWITCH_STATUS_FALSE;
		payload_len = ((uint64_t)header[2] << 8) | (uint64_t)header[3];
		header_len = 4;
	} else if (payload_len == 127) {
		if (recv_exact(*ws_socket, *ssl, &header[2], 8) != 8) return SWITCH_STATUS_FALSE;
		/* Reconstruct 64-bit big-endian length from header[2..9] */
		payload_len = ((uint64_t)header[2] << 56) | ((uint64_t)header[3] << 48) |
		              ((uint64_t)header[4] << 40) | ((uint64_t)header[5] << 32) |
		              ((uint64_t)header[6] << 24) | ((uint64_t)header[7] << 16) |
		              ((uint64_t)header[8] << 8)  | (uint64_t)header[9];
		header_len = 10;
	}

	if (opcode >= 0x8 && payload_len > 125) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Invalid WebSocket control frame payload length: %" PRIu64 "\n", payload_len);
		return SWITCH_STATUS_FALSE;
	}

	if (payload_len > WS_MAX_FRAME_PAYLOAD) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "WebSocket frame too large: %" PRIu64 " bytes\n", payload_len);
		return SWITCH_STATUS_FALSE;
	}

	/* Check masking (server-to-client frames should NOT be masked per RFC 6455) */
	if (header[1] & 0x80) {
		if (recv_exact(*ws_socket, *ssl, header + header_len, 4) != 4) return SWITCH_STATUS_FALSE;
	}

	if (payload_len > 0) {
		/* Allocate buffer */
		*data = (char *)malloc((size_t)payload_len + 1);
		if (!*data) {
			return SWITCH_STATUS_FALSE;
		}

		/* Read payload (all bytes, handling TCP fragmentation) */
		if (recv_exact(*ws_socket, *ssl, *data, (int)payload_len) != (int)payload_len) {
			free(*data);
			*data = NULL;
			return SWITCH_STATUS_FALSE;
		}
		(*data)[payload_len] = '\0'; /* null-terminate (useful for text frames) */

		/* Unmask if needed */
		if (header[1] & 0x80) {
			int i;
			for (i = 0; i < (int)payload_len; i++) {
				(*data)[i] ^= header[header_len + (i % 4)];
			}
		}
	}

	*len = payload_len;

	if (opcode == 0x9) {
		/* Reply to server ping with a masked pong, then let the receive loop continue. */
		ws_send_frame_opcode(session, *data, (size_t)payload_len, is_read_direction, 0xA);
		free(*data);
		*data = NULL;
		*len = 0;
		return SWITCH_STATUS_SUCCESS;
	}

	if (opcode == 0xA) {
		free(*data);
		*data = NULL;
		*len = 0;
		return SWITCH_STATUS_SUCCESS;
	}

	session->bytes_received += payload_len;
	return SWITCH_STATUS_SUCCESS;
}

/* Fire ESL event */
static void ws_media_fire_event(const char *event_name, ws_media_session_t *session, const char *key, const char *value)
{
	switch_event_t *event;
	switch_status_t status;
	
	if (!session || !session->session) {
		return;
	}
	
	status = switch_event_create_subclass(&event, SWITCH_EVENT_CUSTOM, event_name);
	if (status != SWITCH_STATUS_SUCCESS) {
		return;
	}
	
	switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "Unique-ID", session->uuid);
	switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "Call-command", "ws_media");
	
	if (key && value) {
		switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, key, value);
	}
	
	if (session->start_time) {
		char buf[64];
		snprintf(buf, sizeof(buf), "%" PRId64 "", session->start_time);
		switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "Start-Time", buf);
	}
	
	switch_event_fire(&event);
}

/* Update packet loss statistics and check threshold */
static void update_packet_loss_stats(ws_media_session_t *session)
{
	switch_time_t now = switch_micro_time_now();
	switch_time_t elapsed = now - session->last_stats_time;

	/* Update stats every 5 seconds */
	if (elapsed >= 5000000) { /* 5 seconds in microseconds */
		uint64_t frames_sent_delta = session->frames_sent - session->last_frames_sent;
		uint64_t frames_dropped_delta = session->frames_dropped - session->last_frames_dropped;

		if (frames_sent_delta > 0) {
			session->current_packet_loss_rate = (float)frames_dropped_delta / (float)frames_sent_delta;

			/* Log packet loss rate */
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
				"Packet loss rate: %.2f%% (dropped: %" PRIu64 ", sent: %" PRIu64 ")\n",
				session->current_packet_loss_rate * 100.0,
				frames_dropped_delta, frames_sent_delta);

			/* Check if packet loss rate exceeds threshold */
			if (session->current_packet_loss_rate > globals.packet_loss_threshold) {
				if (!session->bypass_mode) {
					switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
						"Packet loss rate %.2f%% exceeds threshold %.2f%%, switching to bypass mode\n",
						session->current_packet_loss_rate * 100.0,
						globals.packet_loss_threshold * 100.0);
					session->bypass_mode = SWITCH_TRUE;
					ws_media_fire_event(WS_MEDIA_EVENT_ERROR, session,
						"Error", "High packet loss rate, bypass mode enabled");
				}
			}
		}

		/* Update last stats */
		session->last_stats_time = now;
		session->last_frames_sent = session->frames_sent;
		session->last_frames_dropped = session->frames_dropped;
	}
}

/* READ direction send thread - sends B's audio to WebSocket (no direction marker) */
static void *SWITCH_THREAD_FUNC read_send_thread(switch_thread_t *thread, void *obj)
{
	ws_media_session_t *session = (ws_media_session_t *)obj;
	switch_size_t data_len;
	char *data = NULL;
	switch_size_t data_cap = 0;
	switch_channel_t *channel;

	if (!session || !session->session) {
		return NULL;
	}

	channel = switch_core_session_get_channel(session->session);
	if (!channel) {
		return NULL;
	}

	while (session->running && switch_channel_up(channel)) {
		/* Update packet loss statistics */
		update_packet_loss_stats(session);

		/* Skip sending if in bypass mode */
		if (session->bypass_mode) {
			switch_yield(100000); /* 100ms */
			continue;
		}

		switch_mutex_lock(session->audio_mutex);

		if (switch_buffer_inuse(session->read_send_buffer) > 0) {
			/* Check queue size - drop frames if too large */
			if (switch_buffer_inuse(session->read_send_buffer) > (switch_size_t)globals.max_queue_size) {
				switch_size_t drop_size = switch_buffer_inuse(session->read_send_buffer) - globals.drop_threshold;
				switch_buffer_toss(session->read_send_buffer, drop_size);
				session->frames_dropped++;
			}

			data_len = switch_buffer_inuse(session->read_send_buffer);
			if (data_len > 0) {
				if (data_len > data_cap) {
					char *new_data = (char *)realloc(data, data_len);
					if (new_data) {
						data = new_data;
						data_cap = data_len;
					}
				}

				if (data && data_cap >= data_len) {
					switch_buffer_read(session->read_send_buffer, data, data_len);
					switch_mutex_unlock(session->audio_mutex);

					/* Send pure audio data, no direction marker */
					if (session->read_connected && ws_send_frame(session, data, data_len, SWITCH_TRUE, SWITCH_FALSE) == SWITCH_STATUS_SUCCESS) {
						session->frames_sent++;
					}

					continue;
				}
			}
		}

		switch_mutex_unlock(session->audio_mutex);
		switch_yield(10000); /* 10ms */
	}

	free(data);
	return NULL;
}

/* WRITE direction send thread - sends A's audio to WebSocket (no direction marker) */
static void *SWITCH_THREAD_FUNC write_send_thread(switch_thread_t *thread, void *obj)
{
	ws_media_session_t *session = (ws_media_session_t *)obj;
	switch_size_t data_len;
	char *data = NULL;
	switch_size_t data_cap = 0;
	switch_channel_t *channel;

	if (!session || !session->session) {
		return NULL;
	}

	channel = switch_core_session_get_channel(session->session);
	if (!channel) {
		return NULL;
	}

	while (session->running && switch_channel_up(channel)) {
		/* Skip sending if in bypass mode */
		if (session->bypass_mode) {
			switch_yield(100000); /* 100ms */
			continue;
		}

		switch_mutex_lock(session->audio_mutex);

		if (switch_buffer_inuse(session->write_send_buffer) > 0) {
			/* Check queue size - drop frames if too large */
			if (switch_buffer_inuse(session->write_send_buffer) > (switch_size_t)globals.max_queue_size) {
				switch_size_t drop_size = switch_buffer_inuse(session->write_send_buffer) - globals.drop_threshold;
				switch_buffer_toss(session->write_send_buffer, drop_size);
				session->frames_dropped++;
			}

			data_len = switch_buffer_inuse(session->write_send_buffer);
			if (data_len > 0) {
				if (data_len > data_cap) {
					char *new_data = (char *)realloc(data, data_len);
					if (new_data) {
						data = new_data;
						data_cap = data_len;
					}
				}

				if (data && data_cap >= data_len) {
					switch_buffer_read(session->write_send_buffer, data, data_len);
					switch_mutex_unlock(session->audio_mutex);

					/* Send pure audio data, no direction marker */
					if (session->write_connected && ws_send_frame(session, data, data_len, SWITCH_FALSE, SWITCH_FALSE) == SWITCH_STATUS_SUCCESS) {
						session->frames_sent++;
					}

					continue;
				}
			}
		}

		switch_mutex_unlock(session->audio_mutex);
		switch_yield(10000); /* 10ms */
	}

	free(data);
	return NULL;
}

/* READ direction receive thread - receives processed audio for A (no direction marker) */
static void *SWITCH_THREAD_FUNC read_recv_thread(switch_thread_t *thread, void *obj)
{
	ws_media_session_t *session = (ws_media_session_t *)obj;
	char *data = NULL;
	size_t len = 0;
	switch_channel_t *channel;

	if (!session || !session->session) {
		return NULL;
	}

	channel = switch_core_session_get_channel(session->session);
	if (!channel) {
		return NULL;
	}

	/* Establish the initial READ connection here so call setup is never blocked.
	 * On failure the reconnect logic in the loop below retries. */
	if (session->running && !session->bypass_mode) {
		ws_connect_read(session);
	}

	while (session->running && switch_channel_up(channel)) {
		uint8_t recv_opcode = 0;
		switch_status_t recv_status;

		/* Skip receiving if in bypass mode */
		if (session->bypass_mode) {
			switch_yield(100000); /* 100ms */
			continue;
		}

		recv_status = ws_recv_frame(session, &data, &len, SWITCH_TRUE, &recv_opcode);
		if (recv_status == SWITCH_STATUS_TIMEOUT) {
			continue;
		}

		if (recv_status == SWITCH_STATUS_SUCCESS) {
			/* Reset retry count on successful receive */
			session->read_retry_count = 0;

			if (!data || len == 0) {
				free(data);
				data = NULL;
				continue;
			}

			/* Only write binary frames (opcode=2) to the audio buffer.
			 * Text frames (opcode=1) such as the server's init_ack are logged and discarded. */
			if (recv_opcode == 0x2) {
				switch_mutex_lock(session->audio_mutex);

				/* Store processed audio in READ recv buffer (for A) */
				if (switch_buffer_inuse(session->read_recv_buffer) > (switch_size_t)globals.max_queue_size) {
					switch_buffer_toss(session->read_recv_buffer, len);
				} else {
					switch_buffer_write(session->read_recv_buffer, data, len);
				}

				switch_mutex_unlock(session->audio_mutex);

				session->frames_received++;
				ws_media_fire_event(WS_MEDIA_EVENT_AUDIO_RECEIVED, session, "Direction", "READ");
			} else {
				/* Non-binary frame (e.g., text init_ack): log and discard */
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
					"READ recv: skipping non-binary frame (opcode=0x%x, len=%" SWITCH_SIZE_T_FMT ")\n",
					recv_opcode, len);
			}

			free(data);
			data = NULL;
		} else {
			if (!session->running) {
				break;
			}

			/* Connection lost, try to reconnect */
			if (session->read_connected) {
				ws_disconnect_read(session);
			}

			/* Check retry count */
			if (session->read_retry_count >= globals.max_retry_count) {
				/* Max retries reached, switch to bypass mode */
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
					"READ WebSocket connection failed after %d retries, switching to bypass mode\n",
					session->read_retry_count);
				session->bypass_mode = SWITCH_TRUE;
				ws_media_fire_event(WS_MEDIA_EVENT_ERROR, session, "Error", "READ max retries reached, bypass mode enabled");
				break;
			}

			session->read_retry_count++;
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
				"READ WebSocket connection lost, retry attempt %d/%d\n",
				session->read_retry_count, globals.max_retry_count);

			switch_yield(globals.reconnect_interval * 1000000); /* Bug fix #3: seconds to microseconds */

			if (session->running && !session->bypass_mode) {
				if (ws_connect_read(session) == SWITCH_STATUS_SUCCESS) {
					session->read_retry_count = 0;
				}
			}
		}
	}

	if (data) {
		free(data);
	}

	return NULL;
}

/* WRITE direction receive thread - receives processed audio for B (no direction marker) */
static void *SWITCH_THREAD_FUNC write_recv_thread(switch_thread_t *thread, void *obj)
{
	ws_media_session_t *session = (ws_media_session_t *)obj;
	char *data = NULL;
	size_t len = 0;
	switch_channel_t *channel;

	if (!session || !session->session) {
		return NULL;
	}

	channel = switch_core_session_get_channel(session->session);
	if (!channel) {
		return NULL;
	}

	/* Establish the initial WRITE connection here so call setup is never blocked.
	 * On failure the reconnect logic in the loop below retries. */
	if (session->running && !session->bypass_mode) {
		ws_connect_write(session);
	}

	while (session->running && switch_channel_up(channel)) {
		uint8_t recv_opcode = 0;
		switch_status_t recv_status;

		/* Skip receiving if in bypass mode */
		if (session->bypass_mode) {
			switch_yield(100000); /* 100ms */
			continue;
		}

		recv_status = ws_recv_frame(session, &data, &len, SWITCH_FALSE, &recv_opcode);
		if (recv_status == SWITCH_STATUS_TIMEOUT) {
			continue;
		}

		if (recv_status == SWITCH_STATUS_SUCCESS) {
			/* Reset retry count on successful receive */
			session->write_retry_count = 0;

			if (!data || len == 0) {
				free(data);
				data = NULL;
				continue;
			}

			/* Only write binary frames (opcode=2) to the audio buffer.
			 * Text frames (opcode=1) such as the server's init_ack are logged and discarded. */
			if (recv_opcode == 0x2) {
				switch_mutex_lock(session->audio_mutex);

				/* Store processed audio in WRITE recv buffer (for B) */
				if (switch_buffer_inuse(session->write_recv_buffer) > (switch_size_t)globals.max_queue_size) {
					switch_buffer_toss(session->write_recv_buffer, len);
				} else {
					switch_buffer_write(session->write_recv_buffer, data, len);
				}

				switch_mutex_unlock(session->audio_mutex);

				session->frames_received++;
				ws_media_fire_event(WS_MEDIA_EVENT_AUDIO_RECEIVED, session, "Direction", "WRITE");
			} else {
				/* Non-binary frame (e.g., text init_ack): log and discard */
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
					"WRITE recv: skipping non-binary frame (opcode=0x%x, len=%" SWITCH_SIZE_T_FMT ")\n",
					recv_opcode, len);
			}

			free(data);
			data = NULL;
		} else {
			if (!session->running) {
				break;
			}

			/* Connection lost, try to reconnect */
			if (session->write_connected) {
				ws_disconnect_write(session);
			}

			/* Check retry count */
			if (session->write_retry_count >= globals.max_retry_count) {
				/* Max retries reached, switch to bypass mode */
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
					"WRITE WebSocket connection failed after %d retries, switching to bypass mode\n",
					session->write_retry_count);
				session->bypass_mode = SWITCH_TRUE;
				ws_media_fire_event(WS_MEDIA_EVENT_ERROR, session, "Error", "WRITE max retries reached, bypass mode enabled");
				break;
			}

			session->write_retry_count++;
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
				"WRITE WebSocket connection lost, retry attempt %d/%d\n",
				session->write_retry_count, globals.max_retry_count);

			switch_yield(globals.reconnect_interval * 1000000); /* Bug fix #3: seconds to microseconds */

			if (session->running && !session->bypass_mode) {
				if (ws_connect_write(session) == SWITCH_STATUS_SUCCESS) {
					session->write_retry_count = 0;
				}
			}
		}
	}

	if (data) {
		free(data);
	}

	return NULL;
}

/* Tear down a ws_media session: stop threads, close sockets, free buffers.
 * Runs exactly once (guarded by cleaned_up); safe to call from either
 * ws_media_stop_app or the media-bug CLOSE callback. */
static void ws_media_cleanup(ws_media_session_t *session)
{
	switch_status_t retval;

	if (!session || session->cleaned_up) {
		return;
	}
	session->cleaned_up = SWITCH_TRUE;

	session->running = SWITCH_FALSE;
	if (!session->stop_time) {
		session->stop_time = switch_micro_time_now();
	}

	/* Unblock any thread parked in recv()/send() so the joins below are prompt. */
	ws_signal_disconnect_read(session);
	ws_signal_disconnect_write(session);

	if (session->read_send_thread) {
		switch_thread_join(&retval, session->read_send_thread);
		session->read_send_thread = NULL;
	}
	if (session->read_recv_thread) {
		switch_thread_join(&retval, session->read_recv_thread);
		session->read_recv_thread = NULL;
	}
	if (session->write_send_thread) {
		switch_thread_join(&retval, session->write_send_thread);
		session->write_send_thread = NULL;
	}
	if (session->write_recv_thread) {
		switch_thread_join(&retval, session->write_recv_thread);
		session->write_recv_thread = NULL;
	}

	/* Full socket/SSL teardown now that no worker thread can touch them. */
	ws_disconnect_read(session);
	ws_disconnect_write(session);

	if (session->read_send_buffer) {
		switch_buffer_destroy(&session->read_send_buffer);
	}
	if (session->read_recv_buffer) {
		switch_buffer_destroy(&session->read_recv_buffer);
	}
	if (session->write_send_buffer) {
		switch_buffer_destroy(&session->write_send_buffer);
	}
	if (session->write_recv_buffer) {
		switch_buffer_destroy(&session->write_recv_buffer);
	}

	ws_media_fire_event(WS_MEDIA_EVENT_STOP, session, NULL, NULL);
}

/* Media bug callback - handles both READ and WRITE directions */
static switch_bool_t ws_media_callback(switch_media_bug_t *bug, void *user_data, switch_abc_type_t type)
{
	ws_media_session_t *session = (ws_media_session_t *)user_data;
	switch_frame_t *frame = NULL;

	if (!session) {
		return SWITCH_FALSE;
	}

	/* Handle INIT event - must return TRUE to allow bug attachment */
	if (type == SWITCH_ABC_TYPE_INIT) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE,
			"Media bug callback INIT event received\n");
		return SWITCH_TRUE;
	}

	/* Handle CLOSE event — always run teardown here (not gated on ->running) so
	 * an abnormal hangup with no explicit ws_media_stop still joins threads and
	 * frees the per-call buffers. Idempotent via cleaned_up. */
	if (type == SWITCH_ABC_TYPE_CLOSE) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE,
			"Media bug callback CLOSE event received\n");
		ws_media_cleanup(session);
		return SWITCH_TRUE;
	}

	if (!session->running) {
		return SWITCH_FALSE;
	}

	/* Bypass mode: audio passes through without processing */
	if (session->bypass_mode) {
		return SWITCH_TRUE;
	}

	/* Serial mode (replace): process and replace audio stream */
	if (session->serial_mode) {
		/* Handle READ direction (B -> A): B's audio to WebSocket, processed audio back to A */
		if (type == SWITCH_ABC_TYPE_READ_REPLACE) {
			frame = switch_core_media_bug_get_read_replace_frame(bug);

			if (frame && frame->data && frame->datalen > 0) {
				/* Send B's audio to WebSocket only if connected */
				if (session->read_connected) {
					switch_mutex_lock(session->audio_mutex);

					if (switch_buffer_inuse(session->read_send_buffer) < (switch_size_t)globals.max_queue_size) {
						switch_buffer_write(session->read_send_buffer, frame->data, frame->datalen);
					} else {
						session->frames_dropped++;
					}

					switch_mutex_unlock(session->audio_mutex);
				}

				/* Inject processed audio for A. The replace frame is valid only
				 * within this callback: overwrite its data in place and set it
				 * back — never cache the pointer across callbacks. */
				switch_mutex_lock(session->audio_mutex);
				if (switch_buffer_inuse(session->read_recv_buffer) >= frame->datalen) {
					switch_buffer_read(session->read_recv_buffer, frame->data, frame->datalen);
					switch_core_media_bug_set_read_replace_frame(bug, frame);
				}
				/* else: not enough processed audio yet — leave the original frame */
				switch_mutex_unlock(session->audio_mutex);
			}
		}
		/* Handle WRITE direction (A -> B): A's audio to WebSocket, processed audio back to B */
		else if (type == SWITCH_ABC_TYPE_WRITE_REPLACE) {
			frame = switch_core_media_bug_get_write_replace_frame(bug);

			if (frame && frame->data && frame->datalen > 0) {
				/* Send A's audio to WebSocket only if connected */
				if (session->write_connected) {
					switch_mutex_lock(session->audio_mutex);

					if (switch_buffer_inuse(session->write_send_buffer) < (switch_size_t)globals.max_queue_size) {
						switch_buffer_write(session->write_send_buffer, frame->data, frame->datalen);
					} else {
						session->frames_dropped++;
					}

					switch_mutex_unlock(session->audio_mutex);
				}

				/* Inject processed audio for B. The replace frame is valid only
				 * within this callback: overwrite its data in place and set it
				 * back — never cache the pointer across callbacks. */
				switch_mutex_lock(session->audio_mutex);
				if (switch_buffer_inuse(session->write_recv_buffer) >= frame->datalen) {
					switch_buffer_read(session->write_recv_buffer, frame->data, frame->datalen);
					switch_core_media_bug_set_write_replace_frame(bug, frame);
				}
				/* else: not enough processed audio yet — leave the original frame */
				switch_mutex_unlock(session->audio_mutex);
			}
		}
	}
	/* Parallel mode (copy): copy and send audio, don't modify original stream */
	else {
		/* Handle READ direction (B -> A): Copy B's audio to WebSocket, don't modify */
		if (type == SWITCH_ABC_TYPE_READ_REPLACE) {
			frame = switch_core_media_bug_get_read_replace_frame(bug);

			if (frame && frame->data && frame->datalen > 0 && session->read_connected) {
				switch_mutex_lock(session->audio_mutex);

				if (switch_buffer_inuse(session->read_send_buffer) < (switch_size_t)globals.max_queue_size) {
					switch_buffer_write(session->read_send_buffer, frame->data, frame->datalen);
				} else {
					session->frames_dropped++;
				}

				switch_mutex_unlock(session->audio_mutex);
			}
		}
		/* Handle WRITE direction (A -> B): Copy A's audio to WebSocket, don't modify */
		else if (type == SWITCH_ABC_TYPE_WRITE_REPLACE) {
			frame = switch_core_media_bug_get_write_replace_frame(bug);

			if (frame && frame->data && frame->datalen > 0 && session->write_connected) {
				switch_mutex_lock(session->audio_mutex);

				if (switch_buffer_inuse(session->write_send_buffer) < (switch_size_t)globals.max_queue_size) {
					switch_buffer_write(session->write_send_buffer, frame->data, frame->datalen);
				} else {
					session->frames_dropped++;
				}

				switch_mutex_unlock(session->audio_mutex);
			}
		}

		/* In parallel mode, always let audio pass through */
		return SWITCH_TRUE;
	}

	return SWITCH_TRUE;
}

/* Start translation application */
SWITCH_STANDARD_APP(ws_media_start_app)
{
	switch_media_bug_t *bug = NULL;
	switch_channel_t *channel;
	ws_media_session_t *ws_session = NULL;
	switch_status_t status = SWITCH_STATUS_FALSE;
	switch_media_bug_flag_t flags;
	switch_core_session_t *fs_session = session;
	const char *ws_mode_str;
	switch_bool_t serial_mode = SWITCH_TRUE;  /* Default to serial mode */
	switch_codec_implementation_t read_impl = {0};
	switch_codec_implementation_t write_impl = {0};

	if (!fs_session) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Session is NULL\n");
		return;
	}

	channel = switch_core_session_get_channel(fs_session);
	if (!channel) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Channel is NULL\n");
		return;
	}

	/* Check if already running */
	bug = (switch_media_bug_t *)switch_channel_get_private(channel, "_ws_media_");
	if (bug) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Media processing already running on channel %s\n", switch_channel_get_name(channel));
		return;
	}

	/* Check channel state */
	if (!switch_channel_media_ready(channel)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
			"Cannot attach media bug: channel %s media not ready. Channel state: %s\n",
			switch_channel_get_name(channel),
			switch_channel_state_name(switch_channel_get_state(channel)));
		return;
	}

	/* Check if channel has media */
	if (switch_channel_test_flag(channel, CF_PROXY_MEDIA)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
			"Cannot attach media bug: channel %s is in proxy_media mode\n",
			switch_channel_get_name(channel));
		return;
	}

	/* Check bypass_media via variable instead of flag */
	if (switch_true(switch_channel_get_variable(channel, "bypass_media"))) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
			"Cannot attach media bug: channel %s has bypass_media=true\n",
			switch_channel_get_name(channel));
		return;
	}

	/* Check if we have valid codec implementations BEFORE allocating session */
	if (switch_core_session_get_read_impl(fs_session, &read_impl) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
			"Cannot attach media bug: no read codec implementation for channel %s\n",
			switch_channel_get_name(channel));
		return;
	}

	if (switch_core_session_get_write_impl(fs_session, &write_impl) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
			"Cannot attach media bug: no write codec implementation for channel %s\n",
			switch_channel_get_name(channel));
		return;
	}

	/* Get processing mode from channel variable: ws_media_mode
	 * "serial" or "true" = serial processing (replace audio)
	 * "parallel" or "false" = parallel processing (copy audio)
	 */
	ws_mode_str = switch_channel_get_variable(channel, "ws_media_mode");
	if (!zstr(ws_mode_str)) {
		if (!strcasecmp(ws_mode_str, "parallel") || !strcasecmp(ws_mode_str, "false")) {
			serial_mode = SWITCH_FALSE;
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
				"WebSocket media mode: PARALLEL (copy)\n");
		} else {
			serial_mode = SWITCH_TRUE;
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
				"WebSocket media mode: SERIAL (replace)\n");
		}
	}

	/* Set media bug flags based on mode */
	if (serial_mode) {
		/* Serial mode: use REPLACE flags to modify audio */
		flags = SMBF_READ_REPLACE | SMBF_WRITE_REPLACE | SMBF_NO_PAUSE;
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE,
			"Serial mode: using SMBF_READ_REPLACE | SMBF_WRITE_REPLACE | SMBF_NO_PAUSE\n");
	} else {
		/* Parallel mode still uses REPLACE callbacks so we can access the
		 * current frame directly, but the callback leaves frame_out unchanged. */
		flags = SMBF_READ_REPLACE | SMBF_WRITE_REPLACE | SMBF_NO_PAUSE;
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE,
			"Parallel mode: using REPLACE callbacks without modifying audio (flags=%d)\n", flags);
	}

	/* Allocate session */
	ws_session = (ws_media_session_t *)switch_core_session_alloc(fs_session, sizeof(ws_media_session_t));
	if (!ws_session) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to allocate session\n");
		return;
	}

	memset(ws_session, 0, sizeof(ws_media_session_t));
	ws_session->session = fs_session;
	ws_session->uuid = switch_core_strdup(switch_core_session_get_pool(fs_session), switch_core_session_get_uuid(fs_session));
	ws_session->read_ws_socket = -1;
	ws_session->write_ws_socket = -1;
	ws_session->running = SWITCH_TRUE;
	ws_session->bypass_mode = SWITCH_FALSE;
	ws_session->serial_mode = serial_mode;
	ws_session->read_retry_count = 0;
	ws_session->write_retry_count = 0;
	ws_session->start_time = switch_micro_time_now();
	ws_session->last_stats_time = switch_micro_time_now();
	ws_session->current_packet_loss_rate = 0.0;
	ws_session->last_frames_sent = 0;
	ws_session->last_frames_dropped = 0;

	/* Get codec info */
	switch_core_session_get_read_impl(fs_session, &ws_session->read_impl);
	switch_core_session_get_write_impl(fs_session, &ws_session->write_impl);

	/* Create buffers for both directions */
	/* READ direction (B -> A) */
	if (switch_buffer_create_dynamic(&ws_session->read_send_buffer, 1024, 8192, 0) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to create read_send_buffer\n");
		goto error;
	}
	if (switch_buffer_create_dynamic(&ws_session->read_recv_buffer, 1024, 8192, 0) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to create read_recv_buffer\n");
		goto error;
	}
	/* WRITE direction (A -> B) */
	if (switch_buffer_create_dynamic(&ws_session->write_send_buffer, 1024, 8192, 0) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to create write_send_buffer\n");
		goto error;
	}
	if (switch_buffer_create_dynamic(&ws_session->write_recv_buffer, 1024, 8192, 0) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to create write_recv_buffer\n");
		goto error;
	}
	if (switch_mutex_init(&ws_session->audio_mutex, SWITCH_MUTEX_NESTED, switch_core_session_get_pool(fs_session)) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to create audio_mutex\n");
		goto error;
	}
	if (switch_mutex_init(&ws_session->read_send_lock, SWITCH_MUTEX_UNNESTED, switch_core_session_get_pool(fs_session)) != SWITCH_STATUS_SUCCESS ||
		switch_mutex_init(&ws_session->write_send_lock, SWITCH_MUTEX_UNNESTED, switch_core_session_get_pool(fs_session)) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to create send locks\n");
		goto error;
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "Buffers and mutex created successfully\n");

	/* Add detailed debug info before attaching media bug */
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE,
		"About to attach media bug - Channel: %s, State: %s, Media ready: %d\n",
		switch_channel_get_name(channel),
		switch_channel_state_name(switch_channel_get_state(channel)),
		switch_channel_media_ready(channel));

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE,
		"Codec info - Read: %s@%dHz, Write: %s@%dHz\n",
		read_impl.iananame, read_impl.samples_per_second,
		write_impl.iananame, write_impl.samples_per_second);

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE,
		"Media bug flags: %d (serial_mode=%d, using SMBF_BOTH=%d)\n",
		flags, serial_mode, (flags == (SMBF_BOTH | SMBF_NO_PAUSE)));

	/* NOTE: the WebSocket connections are established by the receive threads,
	 * NOT here. Connecting on the dialplan/API thread would block call setup for
	 * seconds on a slow/unreachable backend. Each receive thread connects
	 * immediately on start and owns reconnect/bypass for its direction; the send
	 * threads and the media-bug callback only enqueue audio once *_connected. */

	/* Add media bug BEFORE starting threads */
	status = switch_core_media_bug_add(fs_session, "ws_media", NULL, ws_media_callback, ws_session, 0, flags, &bug);
	if (status != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
			"Failed to add media bug (status=%d). "
			"Possible reasons: bypass_media=true, proxy_media=true, or no media stream. "
			"Ensure media flows through FreeSWITCH.\n", status);
		ws_media_fire_event(WS_MEDIA_EVENT_ERROR, ws_session, "Error", "Media bug failed");
		goto error;
	}

	ws_session->bug = bug;
	switch_channel_set_private(channel, "_ws_media_", bug);

	/* Start threads for both directions AFTER media bug is attached */
	switch_thread_create(&ws_session->read_send_thread, NULL, read_send_thread, ws_session, switch_core_session_get_pool(fs_session));
	switch_thread_create(&ws_session->read_recv_thread, NULL, read_recv_thread, ws_session, switch_core_session_get_pool(fs_session));
	switch_thread_create(&ws_session->write_send_thread, NULL, write_send_thread, ws_session, switch_core_session_get_pool(fs_session));
	switch_thread_create(&ws_session->write_recv_thread, NULL, write_recv_thread, ws_session, switch_core_session_get_pool(fs_session));

	ws_media_fire_event(WS_MEDIA_EVENT_START, ws_session, NULL, NULL);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Media processing started on channel %s (mode: %s)\n",
		switch_channel_get_name(channel), serial_mode ? "SERIAL" : "PARALLEL");

	return;

error:
	if (ws_session) {
		switch_status_t retval;

		ws_session->running = SWITCH_FALSE;

		/* Wait for threads to finish if they were started */
		if (ws_session->read_send_thread) {
			switch_thread_join(&retval, ws_session->read_send_thread);
		}
		if (ws_session->read_recv_thread) {
			switch_thread_join(&retval, ws_session->read_recv_thread);
		}
		if (ws_session->write_send_thread) {
			switch_thread_join(&retval, ws_session->write_send_thread);
		}
		if (ws_session->write_recv_thread) {
			switch_thread_join(&retval, ws_session->write_recv_thread);
		}

		/* Disconnect WebSocket */
		if (ws_session->read_ws_socket >= 0) {
			ws_disconnect_read(ws_session);
		}
		if (ws_session->write_ws_socket >= 0) {
			ws_disconnect_write(ws_session);
		}

		/* Cleanup buffers */
		if (ws_session->read_send_buffer) {
			switch_buffer_destroy(&ws_session->read_send_buffer);
		}
		if (ws_session->read_recv_buffer) {
			switch_buffer_destroy(&ws_session->read_recv_buffer);
		}
		if (ws_session->write_send_buffer) {
			switch_buffer_destroy(&ws_session->write_send_buffer);
		}
		if (ws_session->write_recv_buffer) {
			switch_buffer_destroy(&ws_session->write_recv_buffer);
		}
	}
}

/* Stop translation application */
SWITCH_STANDARD_APP(ws_media_stop_app)
{
	switch_media_bug_t *bug = NULL;
	switch_channel_t *channel;
	switch_core_session_t *fs_session = session;

	if (!fs_session) {
		return;
	}

	channel = switch_core_session_get_channel(fs_session);
	if (!channel) {
		return;
	}

	bug = (switch_media_bug_t *)switch_channel_get_private(channel, "_ws_media_");
	if (!bug) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Media processing not running on channel %s\n", switch_channel_get_name(channel));
		return;
	}

	/* Clear the handle, then remove the bug. Removal fires the CLOSE callback,
	 * which runs ws_media_cleanup() — the single teardown path (join threads,
	 * close sockets, free buffers). This is also what runs on an abnormal
	 * hangup where ws_media_stop is never called. */
	switch_channel_set_private(channel, "_ws_media_", NULL);
	switch_core_media_bug_remove(fs_session, &bug);
	
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Media processing stopped on channel %s\n", switch_channel_get_name(channel));
}

/* API function */
SWITCH_STANDARD_API(ws_media_api)
{
	char *mycmd = NULL, *argv[10] = {0};
	int argc = 0;
	
	if (!zstr(cmd) && (mycmd = strdup(cmd))) {
		argc = switch_separate_string(mycmd, ' ', argv, (sizeof(argv) / sizeof(argv[0])));
	}
	
	if (argc < 2) {
		stream->write_function(stream, "-USAGE: uuid_ws_media <uuid> <start|stop>\n");
		goto done;
	}
	
	if (!strcasecmp(argv[1], "start")) {
		switch_core_session_t *fs_session = NULL;

		fs_session = switch_core_session_locate(argv[0]);
		if (fs_session) {
			ws_media_start_app(fs_session, NULL);
			switch_core_session_rwunlock(fs_session);
			stream->write_function(stream, "+OK\n");
		} else {
			stream->write_function(stream, "-ERR Session not found\n");
		}
	} else if (!strcasecmp(argv[1], "stop")) {
		switch_core_session_t *fs_session = NULL;

		fs_session = switch_core_session_locate(argv[0]);
		if (fs_session) {
			ws_media_stop_app(fs_session, NULL);
			switch_core_session_rwunlock(fs_session);
			stream->write_function(stream, "+OK\n");
		} else {
			stream->write_function(stream, "-ERR Session not found\n");
		}
	} else {
		stream->write_function(stream, "-ERR Invalid command\n");
	}
	
done:
	switch_safe_free(mycmd);
	return SWITCH_STATUS_SUCCESS;
}

/* Load configuration */
static switch_status_t load_config(switch_bool_t reload)
{
	switch_xml_t xml, cfg, param;
	switch_memory_pool_t *pool = NULL;
	switch_memory_pool_t *old_pool = globals.config_pool;

	/* Create a new pool for configuration strings */
	if (switch_core_new_memory_pool(&pool) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to create memory pool\n");
		return SWITCH_STATUS_FALSE;
	}

	/* Destroy the previous config pool only after the replacement is ready. */
	if (reload && old_pool) {
		switch_core_destroy_memory_pool(&old_pool);
	}

	memset(&globals, 0, sizeof(globals));

	/* Default configuration */
	globals.ws_host = "localhost";
	globals.ws_port = 8080;
	globals.ws_path = "/media";
	globals.ws_ssl = 0;
	globals.ws_ssl_verify = 0; /* off by default; enable to enforce cert validation */
	globals.ws_auth_user = NULL;
	globals.ws_auth_pass = NULL;
	globals.ws_query_params = NULL;
	globals.max_queue_size = 8192; /* bytes */
	globals.drop_threshold = 4096; /* bytes */
	globals.reconnect_interval = 5; /* seconds */
	globals.max_retry_count = 3; /* default retry count */
	globals.packet_loss_threshold = 0.3; /* 30% packet loss threshold */

	if ((xml = switch_xml_open_cfg("ws_media.conf", &cfg, NULL))) {
		if ((param = switch_xml_child(cfg, "settings"))) {
			switch_xml_t p;
			for (p = switch_xml_child(param, "param"); p; p = p->next) {
				const char *name = switch_xml_attr_soft(p, "name");
				const char *value = switch_xml_attr_soft(p, "value");

				if (!name || !value) continue;

				if (!strcmp(name, "ws-host")) {
					globals.ws_host = switch_core_strdup(pool, value);
				} else if (!strcmp(name, "ws-port")) {
					globals.ws_port = atoi(value);
				} else if (!strcmp(name, "ws-path")) {
					globals.ws_path = switch_core_strdup(pool, value);
				} else if (!strcmp(name, "ws-ssl")) {
					globals.ws_ssl = switch_true(value);
				} else if (!strcmp(name, "ws-ssl-verify")) {
					globals.ws_ssl_verify = switch_true(value);
				} else if (!strcmp(name, "ws-auth-user")) {
					globals.ws_auth_user = switch_core_strdup(pool, value);
				} else if (!strcmp(name, "ws-auth-pass")) {
					globals.ws_auth_pass = switch_core_strdup(pool, value);
				} else if (!strcmp(name, "ws-query-params")) {
					globals.ws_query_params = switch_core_strdup(pool, value);
				} else if (!strcmp(name, "max-queue-size")) {
					globals.max_queue_size = atoi(value);
				} else if (!strcmp(name, "drop-threshold")) {
					globals.drop_threshold = atoi(value);
				} else if (!strcmp(name, "reconnect-interval")) {
					globals.reconnect_interval = atoi(value);
				} else if (!strcmp(name, "max-retry-count")) {
					globals.max_retry_count = atoi(value);
					if (globals.max_retry_count < 1) {
						globals.max_retry_count = 1;
					}
				} else if (!strcmp(name, "packet-loss-threshold")) {
					globals.packet_loss_threshold = atof(value);
					if (globals.packet_loss_threshold < 0.0) {
						globals.packet_loss_threshold = 0.0;
					} else if (globals.packet_loss_threshold > 1.0) {
						globals.packet_loss_threshold = 1.0;
					}
				}
			}
		}
		switch_xml_free(xml);
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
		"mod_ws_media config: host=%s port=%d path=%s ssl=%d auth_user=%s query_params=%s "
		"max_queue=%d drop_threshold=%d reconnect=%d max_retry=%d packet_loss_threshold=%.2f%%\n",
		globals.ws_host, globals.ws_port, globals.ws_path, globals.ws_ssl,
		globals.ws_auth_user ? globals.ws_auth_user : "(none)",
		globals.ws_query_params ? globals.ws_query_params : "(none)",
		globals.max_queue_size, globals.drop_threshold, globals.reconnect_interval,
		globals.max_retry_count, globals.packet_loss_threshold * 100.0);

	/* Keep the pool alive — it backs all the config strings in globals */
	globals.config_pool = pool;

	return SWITCH_STATUS_SUCCESS;
}

/* Load module */
SWITCH_MODULE_LOAD_FUNCTION(mod_ws_media_load)
{
	switch_api_interface_t *api_interface;
	switch_application_interface_t *app_interface;

	*module_interface = switch_loadable_module_create_module_interface(pool, modname);

	/* Load configuration */
	if (load_config(SWITCH_FALSE) != SWITCH_STATUS_SUCCESS) {
		return SWITCH_STATUS_FALSE;
	}

	SWITCH_ADD_API(api_interface, "uuid_ws_media", "WebSocket Media API", ws_media_api, "<uuid> <start|stop>");
	SWITCH_ADD_APP(app_interface, "ws_media_start", "Start media processing", "Start WebSocket media processing", ws_media_start_app, "<args>", SAF_NONE);
	SWITCH_ADD_APP(app_interface, "ws_media_stop", "Stop media processing", "Stop WebSocket media processing", ws_media_stop_app, "", SAF_NONE);

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_ws_media loaded\n");

	return SWITCH_STATUS_SUCCESS;
}

/* Shutdown module */
SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_ws_media_shutdown)
{
	/* Free the configuration memory pool */
	if (globals.config_pool) {
		switch_core_destroy_memory_pool(&globals.config_pool);
	}
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_ws_media shutdown\n");
	return SWITCH_STATUS_SUCCESS;
}
