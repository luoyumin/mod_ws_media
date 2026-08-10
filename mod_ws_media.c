/*
 * FreeSWITCH Modular Media Switching Software Library / Soft-Switch Application
 * Copyright (C) 2024-2026
 *
 * Version: MPL 1.1
 *
 * mod_ws_media.c -- WebSocket media tap for FreeSWITCH (v1.0, "routable media
 *                   bus" line, tap-only milestone).
 *
 * v1.0 scope: one bidirectional WebSocket connection per leg, CAPTURE ONLY
 * (fork/copy out; no injection yet). Captured audio is decoded L16 PCM. Capture
 * source is selectable per call: read | write | mixed | stereo. Configuration
 * is per-call (command args / channel variables). Injection, resampling and
 * cross-leg routing arrive in later milestones (see docs/DESIGN.md).
 *
 * This is a clean rewrite; it does NOT preserve the v0 wire protocol. The
 * low-level WebSocket/TLS plumbing is carried over from the validated v0 code.
 *
 * Authors:
 *   LUOYUMIN <luoyumin@meiqia.com>        -- design & implementation
 *   Claude (Anthropic, Opus 4.8)          -- co-author (v1 rewrite, plumbing)
 */

#include <switch.h>
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
#include <errno.h>
#include <fcntl.h>

SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_ws_media_shutdown);
SWITCH_MODULE_LOAD_FUNCTION(mod_ws_media_load);
SWITCH_MODULE_DEFINITION(mod_ws_media, mod_ws_media_load, mod_ws_media_shutdown, NULL);

SWITCH_STANDARD_API(ws_media_api);
SWITCH_STANDARD_APP(ws_media_start_app);
SWITCH_STANDARD_APP(ws_media_stop_app);

#define WS_MEDIA_EVENT_START        "ws_media::start"
#define WS_MEDIA_EVENT_STOP         "ws_media::stop"
#define WS_MEDIA_EVENT_CONNECTED    "ws_media::connected"
#define WS_MEDIA_EVENT_DISCONNECTED "ws_media::disconnected"
#define WS_MEDIA_EVENT_ERROR        "ws_media::error"

#define WS_GUID "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
#define WS_MAX_FRAME_PAYLOAD (1024 * 1024)
#define WS_MASK_CHUNK_SIZE 4096
#define WS_PRIVATE_KEY "_ws_media_"

/* Relaxed atomics for cross-thread counters. */
#define WS_STAT_INC(x)    __atomic_add_fetch(&(x), 1, __ATOMIC_RELAXED)
#define WS_STAT_ADD(x, n) __atomic_add_fetch(&(x), (uint64_t)(n), __ATOMIC_RELAXED)

/* Capture source: which audio of the tapped leg to stream out. */
typedef enum {
	CAP_READ = 0,  /* far end (what the leg hears) - mono */
	CAP_WRITE,     /* near end (what the leg says)  - mono */
	CAP_MIXED,     /* both, summed into one channel - mono */
	CAP_STEREO     /* both, separated: left=read, right=write - 2ch */
} ws_capture_t;

/* Module-wide defaults (per-call values override these). */
typedef struct {
	char *host;
	int   port;
	char *path;
	int   ssl;
	int   ssl_verify;
	char *auth_user;
	char *auth_pass;
	char *query;
	int   max_queue_size;
	int   drop_threshold;
	int   reconnect_interval;
	int   max_retry_count;
	int   bypass_recovery_sec;
	switch_memory_pool_t *config_pool;
} ws_globals_t;

static ws_globals_t globals = {0};

/* Per-call effective configuration (snapshot taken at attach time, so worker
 * threads never read globals -> reload is safe by construction). */
typedef struct {
	char host[256];
	int  port;
	char path[512];
	int  ssl;
	int  ssl_verify;
	char auth_user[128];
	char auth_pass[128];
	char query[512];
	int  max_queue_size;
	int  drop_threshold;
	int  reconnect_interval;
	int  max_retry_count;
	int  bypass_recovery_sec;
} ws_cfg_t;

typedef struct {
	switch_core_session_t *session;
	switch_media_bug_t *bug;
	char *uuid;
	char *call_id;
	char *role;
	char *custom;              /* opaque JSON passed through to the server */

	ws_capture_t capture;
	int channels;              /* 1, or 2 for stereo */

	ws_cfg_t cfg;

	/* single connection */
	int sock;
	SSL_CTX *ssl_ctx;
	SSL *ssl;
	int connected;             /* 1 once the start frame is sent (ready to stream) */
	int close_sent;            /* a Close frame has gone out on this connection */
	int retry_count;
	switch_mutex_t *send_lock; /* serialize socket writes (audio vs pong/start) */

	/* audio */
	switch_buffer_t *send_buffer;
	switch_mutex_t *audio_mutex;
	switch_codec_implementation_t read_impl;

	/* threads / lifecycle */
	switch_thread_t *send_thread;
	switch_thread_t *recv_thread;
	switch_bool_t running;
	switch_bool_t bypass_mode;
	switch_time_t bypass_since;
	switch_bool_t cleaned_up;

	/* stats (atomic) */
	uint64_t frames_sent;
	uint64_t frames_dropped;
	uint64_t bytes_sent;
	switch_time_t start_time;
} ws_session_t;

/* forward decls */
static switch_status_t ws_connect(ws_session_t *s);
static void ws_disconnect(ws_session_t *s);
static void ws_signal_disconnect(ws_session_t *s);
static switch_status_t ws_send_frame_opcode(ws_session_t *s, const char *data, size_t len, uint8_t opcode);
static switch_status_t ws_recv_control(ws_session_t *s);
static void ws_fire_event(const char *name, ws_session_t *s, const char *key, const char *val);
static void ws_cleanup(ws_session_t *s);

/* ------------------------------------------------------------------ */
/* Reliable stream helpers                                            */
/* ------------------------------------------------------------------ */

static int recv_exact(int sock, SSL *ssl, void *buf, int need)
{
	int total = 0;
	if (need <= 0) return 0;
	while (total < need) {
		int r;
		if (ssl) {
			r = SSL_read(ssl, (char *)buf + total, need - total);
			if (r <= 0) {
				int e = SSL_get_error(ssl, r);
				if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE) return total == 0 ? -2 : -1;
				return -1;
			}
		} else {
			r = recv(sock, (char *)buf + total, need - total, 0);
			if (r < 0 && errno == EINTR) continue;
			if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return total == 0 ? -2 : -1;
			if (r <= 0) return -1;
		}
		total += r;
	}
	return total;
}

static int send_exact(int sock, SSL *ssl, const void *buf, int need)
{
	int total = 0;
	if (need <= 0) return 0;
	while (total < need) {
		int r;
		if (ssl) {
			r = SSL_write(ssl, (const char *)buf + total, need - total);
			if (r <= 0) return -1;
		} else {
#ifdef MSG_NOSIGNAL
			r = send(sock, (const char *)buf + total, need - total, MSG_NOSIGNAL);
#else
			r = send(sock, (const char *)buf + total, need - total, 0);
#endif
			if (r < 0 && errno == EINTR) continue;
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
			int e = SSL_get_error(ssl, r);
			if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE) return 0;
			return -1;
		}
		return r;
	}
	do { r = recv(sock, buf, len, 0); } while (r < 0 && errno == EINTR);
	return r;
}

static void ws_set_socket_options(int sock, int timeout_sec)
{
	struct timeval tv;
	int one = 1;
	tv.tv_sec = timeout_sec > 0 ? timeout_sec : 5;
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
	int flags = fcntl(sock, F_GETFL, 0);
	int ret;
	if (flags < 0) return connect(sock, addr, addrlen);
	if (fcntl(sock, F_SETFL, flags | O_NONBLOCK) < 0) return connect(sock, addr, addrlen);
	ret = connect(sock, addr, addrlen);
	if (ret == 0) { fcntl(sock, F_SETFL, flags); return 0; }
	if (errno == EINPROGRESS) {
		fd_set wfds;
		struct timeval tv;
		FD_ZERO(&wfds);
		FD_SET(sock, &wfds);
		tv.tv_sec = timeout_sec;
		tv.tv_usec = 0;
		do { ret = select(sock + 1, NULL, &wfds, NULL, &tv); } while (ret < 0 && errno == EINTR);
		if (ret > 0) {
			int so_error = 0;
			socklen_t l = sizeof(so_error);
			if (getsockopt(sock, SOL_SOCKET, SO_ERROR, &so_error, &l) == 0 && so_error == 0) {
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

static switch_status_t ws_open_socket(ws_session_t *s)
{
	struct addrinfo hints, *result = NULL, *rp;
	char port[16];
	int gai;

	s->sock = -1;
	snprintf(port, sizeof(port), "%d", s->cfg.port);
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = IPPROTO_TCP;

	if ((gai = getaddrinfo(s->cfg.host, port, &hints, &result)) != 0) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "ws_media: resolve %s failed: %s\n", s->cfg.host, gai_strerror(gai));
		return SWITCH_STATUS_FALSE;
	}
	for (rp = result; rp; rp = rp->ai_next) {
		int sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (sock < 0) continue;
		ws_set_socket_options(sock, s->cfg.reconnect_interval);
		if (ws_connect_with_timeout(sock, rp->ai_addr, (socklen_t)rp->ai_addrlen, s->cfg.reconnect_interval > 0 ? s->cfg.reconnect_interval : 5) == 0) {
			s->sock = sock;
			freeaddrinfo(result);
			return SWITCH_STATUS_SUCCESS;
		}
		close(sock);
	}
	freeaddrinfo(result);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "ws_media: connect %s:%d failed\n", s->cfg.host, s->cfg.port);
	return SWITCH_STATUS_FALSE;
}

/* ------------------------------------------------------------------ */
/* WebSocket handshake                                                */
/* ------------------------------------------------------------------ */

static char *base64_encode(const unsigned char *input, int length)
{
	BIO *bmem, *b64;
	BUF_MEM *bptr;
	char *buff;
	if (!(b64 = BIO_new(BIO_f_base64()))) return NULL;
	if (!(bmem = BIO_new(BIO_s_mem()))) { BIO_free(b64); return NULL; }
	b64 = BIO_push(b64, bmem);
	BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
	if (BIO_write(b64, input, length) <= 0 || BIO_flush(b64) != 1) { BIO_free_all(b64); return NULL; }
	BIO_get_mem_ptr(b64, &bptr);
	if (!bptr) { BIO_free_all(b64); return NULL; }
	if (!(buff = malloc(bptr->length + 1))) { BIO_free_all(b64); return NULL; }
	memcpy(buff, bptr->data, bptr->length);
	buff[bptr->length] = 0;
	BIO_free_all(b64);
	return buff;
}

static char *generate_ws_key(void)
{
	unsigned char key[16];
	if (RAND_bytes(key, sizeof(key)) != 1) return NULL;
	return base64_encode(key, sizeof(key));
}

static switch_bool_t ws_validate_handshake(const char *response, const char *encoded_key)
{
	char accept_src[256];
	unsigned char sha[SHA_DIGEST_LENGTH];
	char *expected = NULL;
	const char *status_end, *status_code, *hdr, *val, *end;
	size_t vlen;
	switch_bool_t ok = SWITCH_FALSE;

	status_end = strstr(response, "\r\n");
	status_code = strstr(response, " 101 ");
	if (!status_end || strncmp(response, "HTTP/", 5) || !status_code || status_code > status_end) return SWITCH_FALSE;
	if (!switch_stristr("upgrade:", response) || !switch_stristr("websocket", response) ||
		!switch_stristr("connection:", response) || !switch_stristr("upgrade", response)) return SWITCH_FALSE;

	snprintf(accept_src, sizeof(accept_src), "%s%s", encoded_key, WS_GUID);
	SHA1((unsigned char *)accept_src, strlen(accept_src), sha);
	if (!(expected = base64_encode(sha, SHA_DIGEST_LENGTH))) return SWITCH_FALSE;

	if (!(hdr = switch_stristr("sec-websocket-accept:", response))) { free(expected); return SWITCH_FALSE; }
	if (!(val = strchr(hdr, ':'))) { free(expected); return SWITCH_FALSE; }
	val++;
	while (*val == ' ' || *val == '\t') val++;
	if (!(end = strpbrk(val, "\r\n"))) { free(expected); return SWITCH_FALSE; }
	vlen = (size_t)(end - val);
	while (vlen > 0 && (val[vlen - 1] == ' ' || val[vlen - 1] == '\t')) vlen--;
	if (strlen(expected) == vlen && !strncmp(val, expected, vlen)) ok = SWITCH_TRUE;
	free(expected);
	return ok;
}

static switch_status_t ws_handshake(ws_session_t *s)
{
	char *key;
	char req[4096], resp[4096], auth[512], path[1024];

	if (!(key = generate_ws_key())) return SWITCH_STATUS_FALSE;
	auth[0] = '\0';

	if (!zstr(s->cfg.auth_user) && !zstr(s->cfg.auth_pass)) {
		char raw[300], *enc;
		snprintf(raw, sizeof(raw), "%s:%s", s->cfg.auth_user, s->cfg.auth_pass);
		if ((enc = base64_encode((unsigned char *)raw, strlen(raw)))) {
			snprintf(auth, sizeof(auth), "Authorization: Basic %s\r\n", enc);
			free(enc);
		}
	}
	if (!zstr(s->cfg.query)) snprintf(path, sizeof(path), "%s?%s", s->cfg.path[0] ? s->cfg.path : "/", s->cfg.query);
	else snprintf(path, sizeof(path), "%s", s->cfg.path[0] ? s->cfg.path : "/");

	snprintf(req, sizeof(req),
		"GET %s HTTP/1.1\r\nHost: %s:%d\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
		"Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n%s\r\n",
		path, s->cfg.host, s->cfg.port, key, auth);

	if (send_exact(s->sock, s->ssl, req, strlen(req)) < 0) { free(key); return SWITCH_STATUS_FALSE; }

	memset(resp, 0, sizeof(resp));
	{
		int total = 0;
		while (total < (int)sizeof(resp) - 1) {
			int r = ws_read_some(s->sock, s->ssl, resp + total, (int)sizeof(resp) - 1 - total);
			if (r <= 0) { free(key); return SWITCH_STATUS_FALSE; }
			total += r;
			resp[total] = '\0';
			if (strstr(resp, "\r\n\r\n")) break;
		}
		if (!strstr(resp, "\r\n\r\n")) { free(key); return SWITCH_STATUS_FALSE; }
	}
	if (!ws_validate_handshake(resp, key)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "ws_media: handshake rejected\n");
		free(key);
		return SWITCH_STATUS_FALSE;
	}
	free(key);
	return SWITCH_STATUS_SUCCESS;
}

/* ------------------------------------------------------------------ */
/* TLS                                                                */
/* ------------------------------------------------------------------ */

static switch_bool_t host_is_ip_literal(const char *h)
{
	if (zstr(h)) return SWITCH_FALSE;
	if (strchr(h, ':')) return SWITCH_TRUE;
	return (strspn(h, "0123456789.") == strlen(h)) ? SWITCH_TRUE : SWITCH_FALSE;
}

static switch_status_t ws_tls_establish(ws_session_t *s)
{
	SSL_CTX *ctx;
	SSL *ssl;

	if (!(ctx = SSL_CTX_new(TLS_client_method()))) return SWITCH_STATUS_FALSE;
	if (s->cfg.ssl_verify) {
		SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, NULL);
		SSL_CTX_set_default_verify_paths(ctx);
	}
	if (!(ssl = SSL_new(ctx))) { SSL_CTX_free(ctx); return SWITCH_STATUS_FALSE; }
	SSL_set_fd(ssl, s->sock);
	if (!host_is_ip_literal(s->cfg.host)) SSL_set_tlsext_host_name(ssl, s->cfg.host);
	if (SSL_connect(ssl) != 1) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "ws_media: TLS handshake failed\n");
		SSL_free(ssl); SSL_CTX_free(ctx);
		return SWITCH_STATUS_FALSE;
	}
	if (s->cfg.ssl_verify && SSL_get_verify_result(ssl) != X509_V_OK) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "ws_media: TLS cert verify failed\n");
		SSL_free(ssl); SSL_CTX_free(ctx);
		return SWITCH_STATUS_FALSE;
	}
	if (!s->cfg.ssl_verify) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "ws_media: TLS cert NOT verified (ws-ssl-verify=false)\n");
	}
	s->ssl_ctx = ctx;
	s->ssl = ssl;
	return SWITCH_STATUS_SUCCESS;
}

/* ------------------------------------------------------------------ */
/* WebSocket frames                                                   */
/* ------------------------------------------------------------------ */

static switch_status_t ws_send_frame_opcode(ws_session_t *s, const char *data, size_t len, uint8_t opcode)
{
	unsigned char frame[14], mask[4], chunk[WS_MASK_CHUNK_SIZE];
	size_t header_len, offset;

	if (len > 0 && !data) return SWITCH_STATUS_FALSE;
	if (opcode >= 0x8 && len > 125) return SWITCH_STATUS_FALSE;
	if (s->sock < 0) return SWITCH_STATUS_FALSE;

	frame[0] = 0x80 | (opcode & 0x0F);
	if (RAND_bytes(mask, sizeof(mask)) != 1) return SWITCH_STATUS_FALSE;

	if (len < 126) {
		frame[1] = 0x80 | (unsigned char)len;
		memcpy(&frame[2], mask, 4);
		header_len = 6;
	} else if (len < 65536) {
		frame[1] = 0x80 | 126;
		frame[2] = (len >> 8) & 0xFF; frame[3] = len & 0xFF;
		memcpy(&frame[4], mask, 4);
		header_len = 8;
	} else {
		uint64_t pl = (uint64_t)len;
		frame[1] = 0x80 | 127;
		frame[2] = (pl >> 56) & 0xFF; frame[3] = (pl >> 48) & 0xFF;
		frame[4] = (pl >> 40) & 0xFF; frame[5] = (pl >> 32) & 0xFF;
		frame[6] = (pl >> 24) & 0xFF; frame[7] = (pl >> 16) & 0xFF;
		frame[8] = (pl >> 8) & 0xFF;  frame[9] = pl & 0xFF;
		memcpy(&frame[10], mask, 4);
		header_len = 14;
	}

	if (s->send_lock) switch_mutex_lock(s->send_lock);
	/* RFC 6455 5.5.1: nothing may follow a Close frame. Checking and setting the
	 * flag inside the lock is what makes that ordering airtight — a data frame
	 * already holding the lock finishes first, and any later one is refused. */
	if (s->close_sent && opcode < 0x8) {
		if (s->send_lock) switch_mutex_unlock(s->send_lock);
		return SWITCH_STATUS_FALSE;
	}
	if (opcode == 0x8) s->close_sent = 1;
	if (send_exact(s->sock, s->ssl, frame, header_len) < 0) {
		if (s->send_lock) switch_mutex_unlock(s->send_lock);
		return SWITCH_STATUS_FALSE;
	}
	for (offset = 0; offset < len; ) {
		size_t clen = len - offset, i;
		if (clen > sizeof(chunk)) clen = sizeof(chunk);
		for (i = 0; i < clen; i++) chunk[i] = ((const unsigned char *)data)[offset + i] ^ mask[(offset + i) % 4];
		if (send_exact(s->sock, s->ssl, chunk, (int)clen) < 0) {
			if (s->send_lock) switch_mutex_unlock(s->send_lock);
			return SWITCH_STATUS_FALSE;
		}
		offset += clen;
	}
	if (s->send_lock) switch_mutex_unlock(s->send_lock);

	WS_STAT_ADD(s->bytes_sent, len);
	return SWITCH_STATUS_SUCCESS;
}

/* Normal closure (RFC 6455 7.4.1). */
#define WS_CLOSE_NORMAL 1000

/* Send a Close frame, at most once per connection.
 *
 * Without this the peer only ever sees the TCP connection vanish, which RFC 6455
 * calls an abnormal closure (status 1006). Since the module sends no
 * application-level "end of stream" message either, the server would have no way
 * to tell a finished call from a crashed FreeSWITCH or a severed network — an ASR
 * backend would happily finalize a truncated transcript as if it were complete. */
static switch_status_t ws_send_close(ws_session_t *s, uint16_t code, const char *reason)
{
	char payload[125];
	size_t len = 2;

	if (!s || s->sock < 0 || !s->connected) return SWITCH_STATUS_FALSE;
	if (s->close_sent) return SWITCH_STATUS_SUCCESS;

	payload[0] = (char)((code >> 8) & 0xFF);
	payload[1] = (char)(code & 0xFF);
	if (!zstr(reason)) {
		size_t rlen = strlen(reason);
		if (rlen > sizeof(payload) - 2) rlen = sizeof(payload) - 2;
		memcpy(payload + 2, reason, rlen);
		len += rlen;
	}

	/* ws_send_frame_opcode() sets ->close_sent under the send lock. */
	return ws_send_frame_opcode(s, payload, len, 0x8);
}

/* Read one control/text/binary frame; auto-reply to ping, echo and report a
 * peer-initiated Close. Binary payloads are ignored in v1 (tap-only, server has
 * nothing to send us). Returns TIMEOUT on idle, BREAK when the peer closed
 * cleanly, FALSE on error. */
static switch_status_t ws_recv_control(ws_session_t *s)
{
	unsigned char header[14];
	uint64_t plen;
	int header_len = 2;
	uint8_t opcode;
	char *payload = NULL;

	if (s->sock < 0) return SWITCH_STATUS_FALSE;
	{
		int r = recv_exact(s->sock, s->ssl, header, 2);
		if (r == -2) return SWITCH_STATUS_TIMEOUT;
		if (r != 2) return SWITCH_STATUS_FALSE;
	}
	opcode = header[0] & 0x0F;
	if (header[0] & 0x70) return SWITCH_STATUS_FALSE;      /* reserved bits */
	if (!(header[0] & 0x80)) return SWITCH_STATUS_FALSE;   /* no fragmentation */

	plen = header[1] & 0x7F;
	if (plen == 126) {
		if (recv_exact(s->sock, s->ssl, &header[2], 2) != 2) return SWITCH_STATUS_FALSE;
		plen = ((uint64_t)header[2] << 8) | header[3];
		header_len = 4;
	} else if (plen == 127) {
		if (recv_exact(s->sock, s->ssl, &header[2], 8) != 8) return SWITCH_STATUS_FALSE;
		plen = ((uint64_t)header[2] << 56) | ((uint64_t)header[3] << 48) | ((uint64_t)header[4] << 40) |
		       ((uint64_t)header[5] << 32) | ((uint64_t)header[6] << 24) | ((uint64_t)header[7] << 16) |
		       ((uint64_t)header[8] << 8)  | (uint64_t)header[9];
		header_len = 10;
	}
	if (opcode >= 0x8 && plen > 125) return SWITCH_STATUS_FALSE;
	if (plen > WS_MAX_FRAME_PAYLOAD) return SWITCH_STATUS_FALSE;
	if (header[1] & 0x80) { if (recv_exact(s->sock, s->ssl, header + header_len, 4) != 4) return SWITCH_STATUS_FALSE; }

	if (plen > 0) {
		if (!(payload = malloc((size_t)plen + 1))) return SWITCH_STATUS_FALSE;
		if (recv_exact(s->sock, s->ssl, payload, (int)plen) != (int)plen) { free(payload); return SWITCH_STATUS_FALSE; }
		payload[plen] = '\0';
		if (header[1] & 0x80) { int i; for (i = 0; i < (int)plen; i++) payload[i] ^= header[header_len + (i % 4)]; }
	}

	if (opcode == 0x8) {  /* close -> echo a close back (RFC 6455 5.5.1) */
		uint16_t peer_code = (plen >= 2)
			? (uint16_t)((((unsigned char)payload[0]) << 8) | ((unsigned char)payload[1]))
			: 0;
		uint16_t echo = peer_code;

		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE,
			"ws_media: peer closed the stream (code %u%s%s) on %s\n",
			peer_code ? peer_code : 1005,
			(plen > 2) ? ", reason: " : "", (plen > 2) ? payload + 2 : "", s->uuid);

		/* 1004/1005/1006/1015 must never appear on the wire, and neither may
		 * anything below 1000, so fall back to a plain normal closure. */
		if (echo < 1000 || echo == 1004 || echo == 1005 || echo == 1006 || echo == 1015) echo = WS_CLOSE_NORMAL;
		ws_send_close(s, echo, NULL);

		switch_safe_free(payload);
		return SWITCH_STATUS_BREAK;
	}

	if (opcode == 0x9) {  /* ping -> pong */
		ws_send_frame_opcode(s, payload, (size_t)plen, 0xA);
	}
	/* opcode 0x1 (text) / 0x2 (binary) / 0xA (pong): ignored in tap-only v1 */
	switch_safe_free(payload);
	return SWITCH_STATUS_SUCCESS;
}

/* ------------------------------------------------------------------ */
/* Connect / start packet / disconnect                                */
/* ------------------------------------------------------------------ */

static const char *cap_name(ws_capture_t c)
{
	switch (c) {
	case CAP_READ:  return "read";
	case CAP_WRITE: return "write";
	case CAP_MIXED: return "mixed";
	case CAP_STEREO:return "stereo";
	default:        return "read";
	}
}

static switch_status_t ws_send_start(ws_session_t *s)
{
	char pkt[1536];
	char tracks[512];
	int rate = s->read_impl.actual_samples_per_second;
	int ptime = s->read_impl.microseconds_per_packet / 1000;
	const char *self_role = s->role ? s->role : "self";

	/* Direction semantics (see switch_core_media_bug_read):
	 *   READ  = the party ON THIS leg  (its own microphone)  -> "self" role
	 *   WRITE = the far party (what this leg hears)           -> "peer" role
	 * With SMBF_STEREO: left = read, right = write. */
	if (s->capture == CAP_STEREO) {
		snprintf(tracks, sizeof(tracks),
			"[{\"ch\":0,\"source\":\"read\",\"role\":\"%s\"},"
			"{\"ch\":1,\"source\":\"write\",\"role\":\"peer\"}]",
			self_role);
	} else {
		const char *role = (s->capture == CAP_READ) ? self_role :
		                   (s->capture == CAP_WRITE) ? "peer" : "mixed";
		snprintf(tracks, sizeof(tracks),
			"[{\"ch\":0,\"source\":\"%s\",\"role\":\"%s\"}]",
			cap_name(s->capture), role);
	}

	snprintf(pkt, sizeof(pkt),
		"{\"event\":\"start\",\"version\":\"1\",\"call_id\":\"%s\",\"leg_uuid\":\"%s\","
		"\"attach_mode\":\"tap\","
		"\"media_format\":{\"encoding\":\"L16\",\"sample_rate\":%d,\"channels\":%d,\"ptime\":%d},"
		"\"capture\":{\"mode\":\"%s\",\"tracks\":%s},"
		"\"custom\":%s}",
		s->call_id ? s->call_id : s->uuid, s->uuid,
		rate, s->channels, ptime,
		cap_name(s->capture), tracks,
		(s->custom && s->custom[0]) ? s->custom : "{}");

	return ws_send_frame_opcode(s, pkt, strlen(pkt), 0x1);
}

/* Counterpart to the start frame (docs/DESIGN.md 7.5). Carries call_id/leg_uuid
 * so a server multiplexing several streams knows which one just ended, and a
 * reason so it can tell a finished call from a backend it dropped itself. */
static switch_status_t ws_send_stop(ws_session_t *s, const char *reason)
{
	char pkt[512];

	if (!s || s->sock < 0 || !s->connected) return SWITCH_STATUS_FALSE;

	snprintf(pkt, sizeof(pkt),
		"{\"event\":\"stop\",\"version\":\"1\",\"call_id\":\"%s\",\"leg_uuid\":\"%s\",\"reason\":\"%s\"}",
		s->call_id ? s->call_id : s->uuid, s->uuid, reason ? reason : "call_ended");

	return ws_send_frame_opcode(s, pkt, strlen(pkt), 0x1);
}

static switch_status_t ws_connect(ws_session_t *s)
{
	s->close_sent = 0;   /* fresh connection: nothing closed yet (matters after a bypass recovery) */
	if (ws_open_socket(s) != SWITCH_STATUS_SUCCESS) return SWITCH_STATUS_FALSE;
	if (s->cfg.ssl) {
		if (ws_tls_establish(s) != SWITCH_STATUS_SUCCESS) { close(s->sock); s->sock = -1; return SWITCH_STATUS_FALSE; }
	}
	if (ws_handshake(s) != SWITCH_STATUS_SUCCESS) { ws_disconnect(s); return SWITCH_STATUS_FALSE; }

	/* Send the start frame BEFORE marking connected, so the send thread cannot
	 * emit audio ahead of it (send thread gates on ->connected). */
	if (ws_send_start(s) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "ws_media: start frame not sent\n");
	}
	s->connected = 1;
	ws_fire_event(WS_MEDIA_EVENT_CONNECTED, s, NULL, NULL);
	return SWITCH_STATUS_SUCCESS;
}

static void ws_disconnect(ws_session_t *s)
{
	s->connected = 0;
	/* close_notify has to go out before the socket is torn down, otherwise
	 * SSL_shutdown() is a silent no-op and the peer's TLS stack reports an
	 * unexpected EOF. (In the graceful path ws_cleanup() has already shut the
	 * socket down to wake the recv thread, so this only helps the paths where
	 * it hasn't — see the note in ws_cleanup.) */
	if (s->ssl) { SSL_shutdown(s->ssl); SSL_free(s->ssl); s->ssl = NULL; }
	if (s->ssl_ctx) { SSL_CTX_free(s->ssl_ctx); s->ssl_ctx = NULL; }
	if (s->sock >= 0) { shutdown(s->sock, SHUT_RDWR); close(s->sock); s->sock = -1; }
	ws_fire_event(WS_MEDIA_EVENT_DISCONNECTED, s, NULL, NULL);
}

static void ws_signal_disconnect(ws_session_t *s)
{
	s->connected = 0;
	if (s->sock >= 0) shutdown(s->sock, SHUT_RDWR);
}

/* ------------------------------------------------------------------ */
/* Events                                                             */
/* ------------------------------------------------------------------ */

static void ws_fire_event(const char *name, ws_session_t *s, const char *key, const char *val)
{
	switch_event_t *event;
	if (!s || !s->session) return;
	if (switch_event_create_subclass(&event, SWITCH_EVENT_CUSTOM, name) != SWITCH_STATUS_SUCCESS) return;
	switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "Unique-ID", s->uuid);
	if (s->call_id) switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "Call-ID", s->call_id);
	if (key && val) switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, key, val);
	switch_event_fire(&event);
}

/* ------------------------------------------------------------------ */
/* Threads                                                            */
/* ------------------------------------------------------------------ */

static switch_bool_t ws_bypass_try_recover(ws_session_t *s)
{
	switch_time_t now;
	if (!s->bypass_mode) return SWITCH_TRUE;
	if (s->cfg.bypass_recovery_sec <= 0) return SWITCH_FALSE;
	now = switch_micro_time_now();
	if (s->bypass_since == 0 || (now - s->bypass_since) < (switch_time_t)s->cfg.bypass_recovery_sec * 1000000) return SWITCH_FALSE;
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "ws_media: leaving bypass to retry backend\n");
	s->bypass_mode = SWITCH_FALSE;
	s->bypass_since = 0;
	s->retry_count = 0;
	return SWITCH_TRUE;
}

/* Owns the connection: connect, drain control frames, reconnect, bypass. */
static void *SWITCH_THREAD_FUNC recv_thread(switch_thread_t *thread, void *obj)
{
	ws_session_t *s = (ws_session_t *)obj;
	switch_channel_t *channel;

	if (!s || !s->session) return NULL;
	channel = switch_core_session_get_channel(s->session);
	if (!channel) return NULL;

	if (s->running && !s->bypass_mode) ws_connect(s);

	while (s->running && switch_channel_up(channel)) {
		switch_status_t st;

		if (s->bypass_mode && !ws_bypass_try_recover(s)) { switch_yield(100000); continue; }

		if (!s->connected) {
			if (s->retry_count >= s->cfg.max_retry_count) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "ws_media: max retries, entering bypass\n");
				s->bypass_mode = SWITCH_TRUE;
				s->bypass_since = switch_micro_time_now();
				ws_fire_event(WS_MEDIA_EVENT_ERROR, s, "Error", "max retries, bypass");
				continue;
			}
			s->retry_count++;
			switch_yield((switch_time_t)(s->cfg.reconnect_interval > 0 ? s->cfg.reconnect_interval : 5) * 1000000);
			if (s->running && !s->bypass_mode && ws_connect(s) == SWITCH_STATUS_SUCCESS) s->retry_count = 0;
			continue;
		}

		st = ws_recv_control(s);
		if (st == SWITCH_STATUS_TIMEOUT) continue;
		if (st == SWITCH_STATUS_BREAK) {
			/* The peer asked us to go away (drain, redeploy, quota). Honour it:
			 * reconnecting straight away would turn a graceful drain into a
			 * flap. bypass-recovery-interval decides if and when we come back
			 * (0 = stay in bypass for the rest of the call). */
			if (!s->running) break;
			ws_disconnect(s);
			s->bypass_mode = SWITCH_TRUE;
			s->bypass_since = switch_micro_time_now();
			continue;
		}
		if (st != SWITCH_STATUS_SUCCESS) {
			if (!s->running) break;
			ws_disconnect(s);   /* loop will reconnect */
		}
	}
	return NULL;
}

/* Drains the capture buffer to the WebSocket. */
static void *SWITCH_THREAD_FUNC send_thread(switch_thread_t *thread, void *obj)
{
	ws_session_t *s = (ws_session_t *)obj;
	switch_channel_t *channel;
	char *buf = NULL;
	switch_size_t cap = 0;

	if (!s || !s->session) return NULL;
	channel = switch_core_session_get_channel(s->session);
	if (!channel) return NULL;

	while (s->running && switch_channel_up(channel)) {
		switch_size_t len;

		if (s->bypass_mode || !s->connected) { switch_yield(50000); continue; }

		switch_mutex_lock(s->audio_mutex);
		if (switch_buffer_inuse(s->send_buffer) > 0) {
			if (switch_buffer_inuse(s->send_buffer) > (switch_size_t)s->cfg.max_queue_size) {
				switch_size_t drop = switch_buffer_inuse(s->send_buffer) - s->cfg.drop_threshold;
				switch_buffer_toss(s->send_buffer, drop);
				WS_STAT_INC(s->frames_dropped);
			}
			len = switch_buffer_inuse(s->send_buffer);
			if (len > cap) {
				char *nb = realloc(buf, len);
				if (nb) { buf = nb; cap = len; }
			}
			if (buf && cap >= len) {
				switch_buffer_read(s->send_buffer, buf, len);
				switch_mutex_unlock(s->audio_mutex);
				if (s->connected && ws_send_frame_opcode(s, buf, len, 0x2) == SWITCH_STATUS_SUCCESS) WS_STAT_INC(s->frames_sent);
				continue;
			}
		}
		switch_mutex_unlock(s->audio_mutex);
		switch_yield(10000); /* 10ms */
	}
	switch_safe_free(buf);
	return NULL;
}

/* ------------------------------------------------------------------ */
/* Media bug callback (capture)                                       */
/* ------------------------------------------------------------------ */

static switch_bool_t ws_capture_callback(switch_media_bug_t *bug, void *user_data, switch_abc_type_t type)
{
	ws_session_t *s = (ws_session_t *)user_data;

	if (!s) return SWITCH_FALSE;

	switch (type) {
	case SWITCH_ABC_TYPE_INIT:
		break;
	case SWITCH_ABC_TYPE_CLOSE:
		ws_cleanup(s);
		break;
	case SWITCH_ABC_TYPE_READ:
		if (s->running && !s->bypass_mode && s->connected) {
			uint8_t data[SWITCH_RECOMMENDED_BUFFER_SIZE];
			switch_frame_t frame = { 0 };
			frame.data = data;
			frame.buflen = SWITCH_RECOMMENDED_BUFFER_SIZE;
			while (switch_core_media_bug_read(bug, &frame, SWITCH_FALSE) == SWITCH_STATUS_SUCCESS &&
			       !switch_test_flag((&frame), SFF_CNG)) {
				if (!frame.datalen) continue;
				switch_mutex_lock(s->audio_mutex);
				if (switch_buffer_inuse(s->send_buffer) < (switch_size_t)s->cfg.max_queue_size) {
					switch_buffer_write(s->send_buffer, frame.data, frame.datalen);
				} else {
					WS_STAT_INC(s->frames_dropped);
				}
				switch_mutex_unlock(s->audio_mutex);
			}
		}
		break;
	default:
		break;
	}
	return SWITCH_TRUE;
}

/* ------------------------------------------------------------------ */
/* Cleanup                                                            */
/* ------------------------------------------------------------------ */

static void ws_cleanup(ws_session_t *s)
{
	switch_status_t retval;
	if (!s || s->cleaned_up) return;
	s->cleaned_up = SWITCH_TRUE;
	s->running = SWITCH_FALSE;

	/* Say goodbye while the socket is still open: the stop frame first (it is a
	 * data frame, so it has to precede the Close), then Close(1000). Both have to
	 * happen before ws_signal_disconnect(), which shuts the socket down — doing
	 * it the other way round is why the peer never saw either one and reported an
	 * abnormal 1006 closure on every normal hangup.
	 * The send thread is deliberately not joined first: ->close_sent (set under
	 * the send lock) already stops it from emitting anything after the Close, and
	 * joining here would expose this path to a 5s SO_SNDTIMEO on a wedged peer.
	 * If the peer closed on us, ->close_sent is already set and both calls below
	 * turn into no-ops, which is what RFC 6455 5.5.1 asks for. */
	ws_send_stop(s, "call_ended");
	ws_send_close(s, WS_CLOSE_NORMAL, "call_ended");

	ws_signal_disconnect(s);
	if (s->send_thread) { switch_thread_join(&retval, s->send_thread); s->send_thread = NULL; }
	if (s->recv_thread) { switch_thread_join(&retval, s->recv_thread); s->recv_thread = NULL; }

	ws_disconnect(s);

	if (s->send_buffer) switch_buffer_destroy(&s->send_buffer);

	ws_fire_event(WS_MEDIA_EVENT_STOP, s, NULL, NULL);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "ws_media: stopped on %s\n", s->uuid);
}

/* ------------------------------------------------------------------ */
/* Config resolution (per call)                                       */
/* ------------------------------------------------------------------ */

/* Parse ws://host:port/path or wss://host:port/path into cfg. */
static void parse_ws_url(const char *url, ws_cfg_t *cfg)
{
	const char *p = url, *host_start, *path_start;
	char hostport[300];
	size_t n;

	if (!strncasecmp(p, "wss://", 6)) { cfg->ssl = 1; p += 6; }
	else if (!strncasecmp(p, "ws://", 5)) { cfg->ssl = 0; p += 5; }

	host_start = p;
	path_start = strchr(p, '/');
	if (path_start) {
		n = (size_t)(path_start - host_start);
		if (n >= sizeof(hostport)) n = sizeof(hostport) - 1;
		memcpy(hostport, host_start, n);
		hostport[n] = '\0';
		switch_snprintf(cfg->path, sizeof(cfg->path), "%s", path_start);
	} else {
		switch_snprintf(hostport, sizeof(hostport), "%s", host_start);
		switch_snprintf(cfg->path, sizeof(cfg->path), "/");
	}
	{
		char *colon = strrchr(hostport, ':');
		if (colon && !strchr(colon, ']')) {  /* naive; not IPv6-literal aware */
			*colon = '\0';
			cfg->port = atoi(colon + 1);
		} else {
			cfg->port = cfg->ssl ? 443 : 80;
		}
		switch_snprintf(cfg->host, sizeof(cfg->host), "%s", hostport);
	}
}

static ws_capture_t parse_capture(const char *v)
{
	if (zstr(v)) return CAP_READ;
	if (!strcasecmp(v, "write")) return CAP_WRITE;
	if (!strcasecmp(v, "mixed")) return CAP_MIXED;
	if (!strcasecmp(v, "stereo")) return CAP_STEREO;
	return CAP_READ;
}

/* Resolve effective config from globals + channel variables + api args. */
static void resolve_cfg(switch_channel_t *channel, ws_session_t *s, int argc, char **argv)
{
	const char *v;
	int i;

	/* defaults from globals */
	switch_snprintf(s->cfg.host, sizeof(s->cfg.host), "%s", globals.host ? globals.host : "localhost");
	s->cfg.port = globals.port;
	switch_snprintf(s->cfg.path, sizeof(s->cfg.path), "%s", globals.path ? globals.path : "/media");
	s->cfg.ssl = globals.ssl;
	s->cfg.ssl_verify = globals.ssl_verify;
	if (globals.auth_user) switch_snprintf(s->cfg.auth_user, sizeof(s->cfg.auth_user), "%s", globals.auth_user);
	if (globals.auth_pass) switch_snprintf(s->cfg.auth_pass, sizeof(s->cfg.auth_pass), "%s", globals.auth_pass);
	if (globals.query) switch_snprintf(s->cfg.query, sizeof(s->cfg.query), "%s", globals.query);
	s->cfg.max_queue_size = globals.max_queue_size;
	s->cfg.drop_threshold = globals.drop_threshold;
	s->cfg.reconnect_interval = globals.reconnect_interval;
	s->cfg.max_retry_count = globals.max_retry_count;
	s->cfg.bypass_recovery_sec = globals.bypass_recovery_sec;
	s->capture = CAP_READ;

	/* channel variables */
	if ((v = switch_channel_get_variable(channel, "ws_media_url"))) parse_ws_url(v, &s->cfg);
	if ((v = switch_channel_get_variable(channel, "ws_media_in"))) s->capture = parse_capture(v);
	if ((v = switch_channel_get_variable(channel, "ws_media_role"))) s->role = switch_core_session_strdup(s->session, v);
	if ((v = switch_channel_get_variable(channel, "ws_media_call_id"))) s->call_id = switch_core_session_strdup(s->session, v);
	if ((v = switch_channel_get_variable(channel, "ws_media_meta"))) s->custom = switch_core_session_strdup(s->session, v);

	/* api args: first bare token = url; key=val pairs override */
	for (i = 0; i < argc; i++) {
		char *a = argv[i];
		if (!strncasecmp(a, "ws://", 5) || !strncasecmp(a, "wss://", 6)) { parse_ws_url(a, &s->cfg); continue; }
		if (!strncasecmp(a, "in=", 3)) { s->capture = parse_capture(a + 3); continue; }
		if (!strncasecmp(a, "role=", 5)) { s->role = switch_core_session_strdup(s->session, a + 5); continue; }
		if (!strncasecmp(a, "call_id=", 8)) { s->call_id = switch_core_session_strdup(s->session, a + 8); continue; }
	}

	s->channels = (s->capture == CAP_STEREO) ? 2 : 1;
	if (!s->call_id) s->call_id = s->uuid;
}

static switch_media_bug_flag_t capture_flags(ws_capture_t c)
{
	switch_media_bug_flag_t f = SMBF_READ_PING;
	switch (c) {
	case CAP_READ:   f |= SMBF_READ_STREAM; break;
	case CAP_WRITE:  f |= SMBF_WRITE_STREAM; break;
	case CAP_MIXED:  f |= SMBF_READ_STREAM | SMBF_WRITE_STREAM; break;
	case CAP_STEREO: f |= SMBF_READ_STREAM | SMBF_WRITE_STREAM | SMBF_STEREO; break;
	}
	return f;
}

/* ------------------------------------------------------------------ */
/* start / stop                                                       */
/* ------------------------------------------------------------------ */

/* Returns SWITCH_STATUS_SUCCESS on attach, SWITCH_STATUS_INUSE if this leg
 * already has a tap, or SWITCH_STATUS_FALSE on any other failure. */
static switch_status_t ws_media_start(switch_core_session_t *fs, int argc, char **argv)
{
	switch_channel_t *channel = switch_core_session_get_channel(fs);
	switch_media_bug_t *bug = NULL;
	ws_session_t *s;
	switch_media_bug_flag_t flags;
	switch_memory_pool_t *pool = switch_core_session_get_pool(fs);

	if (switch_channel_get_private(channel, WS_PRIVATE_KEY)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
			"ws_media: already running on %s (one tap per leg; stop it first)\n", switch_channel_get_name(channel));
		return SWITCH_STATUS_INUSE;
	}
	if (!switch_channel_media_ready(channel)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "ws_media: media not ready on %s\n", switch_channel_get_name(channel));
		return SWITCH_STATUS_FALSE;
	}
	if (switch_channel_test_flag(channel, CF_PROXY_MEDIA) || switch_true(switch_channel_get_variable(channel, "bypass_media"))) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "ws_media: proxy/bypass media, cannot attach on %s\n", switch_channel_get_name(channel));
		return SWITCH_STATUS_FALSE;
	}

	s = switch_core_session_alloc(fs, sizeof(*s));
	memset(s, 0, sizeof(*s));
	s->session = fs;
	s->uuid = switch_core_session_strdup(fs, switch_core_session_get_uuid(fs));
	s->sock = -1;
	s->running = SWITCH_TRUE;

	if (switch_core_session_get_read_impl(fs, &s->read_impl) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "ws_media: no read codec impl on %s\n", switch_channel_get_name(channel));
		return SWITCH_STATUS_FALSE;
	}

	resolve_cfg(channel, s, argc, argv);
	s->start_time = switch_micro_time_now();

	if (switch_buffer_create_dynamic(&s->send_buffer, 1024, 8192, 0) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "ws_media: buffer alloc failed\n");
		return SWITCH_STATUS_FALSE;
	}
	switch_mutex_init(&s->audio_mutex, SWITCH_MUTEX_NESTED, pool);
	switch_mutex_init(&s->send_lock, SWITCH_MUTEX_UNNESTED, pool);

	flags = capture_flags(s->capture);
	if (switch_core_media_bug_add(fs, "ws_media", NULL, ws_capture_callback, s, 0, flags, &bug) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "ws_media: media_bug_add failed on %s\n", switch_channel_get_name(channel));
		switch_buffer_destroy(&s->send_buffer);
		return SWITCH_STATUS_FALSE;
	}
	s->bug = bug;
	switch_channel_set_private(channel, WS_PRIVATE_KEY, bug);

	switch_threadattr_t *thd_attr = NULL;
	switch_threadattr_create(&thd_attr, pool);
	switch_threadattr_detach_set(thd_attr, 0);
	switch_thread_create(&s->recv_thread, thd_attr, recv_thread, s, pool);
	switch_threadattr_create(&thd_attr, pool);
	switch_thread_create(&s->send_thread, thd_attr, send_thread, s, pool);

	ws_fire_event(WS_MEDIA_EVENT_START, s, NULL, NULL);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
		"ws_media: started on %s -> %s://%s:%d%s in=%s ch=%d\n",
		switch_channel_get_name(channel), s->cfg.ssl ? "wss" : "ws",
		s->cfg.host, s->cfg.port, s->cfg.path, cap_name(s->capture), s->channels);
	return SWITCH_STATUS_SUCCESS;
}

/* Returns SWITCH_STATUS_SUCCESS if a tap was removed, SWITCH_STATUS_FALSE if
 * none was running on this leg. */
static switch_status_t ws_media_stop(switch_core_session_t *fs)
{
	switch_channel_t *channel = switch_core_session_get_channel(fs);
	switch_media_bug_t *bug = switch_channel_get_private(channel, WS_PRIVATE_KEY);
	if (!bug) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "ws_media: not running on %s\n", switch_channel_get_name(channel));
		return SWITCH_STATUS_FALSE;
	}
	switch_channel_set_private(channel, WS_PRIVATE_KEY, NULL);
	switch_core_media_bug_remove(fs, &bug);   /* fires CLOSE -> ws_cleanup */
	return SWITCH_STATUS_SUCCESS;
}

SWITCH_STANDARD_APP(ws_media_start_app)
{
	char *mydata = NULL, *argv[16] = {0};
	int argc = 0;
	if (!zstr(data) && (mydata = switch_core_session_strdup(session, data))) {
		argc = switch_separate_string(mydata, ' ', argv, (sizeof(argv) / sizeof(argv[0])));
	}
	ws_media_start(session, argc, argv);
}

SWITCH_STANDARD_APP(ws_media_stop_app)
{
	ws_media_stop(session);
}

SWITCH_STANDARD_API(ws_media_api)
{
	char *mycmd = NULL, *argv[16] = {0};
	int argc = 0;

	if (!zstr(cmd) && (mycmd = strdup(cmd))) argc = switch_separate_string(mycmd, ' ', argv, (sizeof(argv) / sizeof(argv[0])));
	if (argc < 2) { stream->write_function(stream, "-USAGE: uuid_ws_media <uuid> <start [ws-url] [in=..] [role=..] [call_id=..] | stop>\n"); goto done; }

	{
		switch_core_session_t *fs = switch_core_session_locate(argv[0]);
		if (!fs) { stream->write_function(stream, "-ERR No such session\n"); goto done; }
		if (!strcasecmp(argv[1], "start")) {
			switch_status_t st = ws_media_start(fs, argc - 2, &argv[2]);
			if (st == SWITCH_STATUS_SUCCESS) {
				stream->write_function(stream, "+OK\n");
			} else if (st == SWITCH_STATUS_INUSE) {
				stream->write_function(stream, "-ERR already running on this leg (one tap per leg; stop it first)\n");
			} else {
				stream->write_function(stream, "-ERR failed to start (check media path / see log)\n");
			}
		} else if (!strcasecmp(argv[1], "stop")) {
			if (ws_media_stop(fs) == SWITCH_STATUS_SUCCESS) {
				stream->write_function(stream, "+OK\n");
			} else {
				stream->write_function(stream, "-ERR not running on this leg\n");
			}
		} else {
			stream->write_function(stream, "-ERR Invalid command\n");
		}
		switch_core_session_rwunlock(fs);
	}
done:
	switch_safe_free(mycmd);
	return SWITCH_STATUS_SUCCESS;
}

/* ------------------------------------------------------------------ */
/* Config / load / shutdown                                           */
/* ------------------------------------------------------------------ */

static switch_status_t load_config(switch_bool_t reload)
{
	switch_xml_t xml, cfg, settings, param;
	switch_memory_pool_t *pool = NULL;

	if (switch_core_new_memory_pool(&pool) != SWITCH_STATUS_SUCCESS) return SWITCH_STATUS_FALSE;

	/* Do NOT free the old pool on reload: in-flight calls snapshot config into
	 * their session, but the pool backs global strings read at attach time.
	 * A small per-reload leak is the safe choice. */
	(void)reload;

	memset(&globals, 0, sizeof(globals));
	globals.host = "localhost";
	globals.port = 8080;
	globals.path = "/media";
	globals.ssl = 0;
	globals.ssl_verify = 0;
	globals.max_queue_size = 8192;
	globals.drop_threshold = 4096;
	globals.reconnect_interval = 5;
	globals.max_retry_count = 3;
	globals.bypass_recovery_sec = 30;

	if ((xml = switch_xml_open_cfg("ws_media.conf", &cfg, NULL))) {
		if ((settings = switch_xml_child(cfg, "settings"))) {
			for (param = switch_xml_child(settings, "param"); param; param = param->next) {
				const char *name = switch_xml_attr_soft(param, "name");
				const char *value = switch_xml_attr_soft(param, "value");
				if (!name || !value) continue;
				if (!strcmp(name, "ws-host")) globals.host = switch_core_strdup(pool, value);
				else if (!strcmp(name, "ws-port")) globals.port = atoi(value);
				else if (!strcmp(name, "ws-path")) globals.path = switch_core_strdup(pool, value);
				else if (!strcmp(name, "ws-ssl")) globals.ssl = switch_true(value);
				else if (!strcmp(name, "ws-ssl-verify")) globals.ssl_verify = switch_true(value);
				else if (!strcmp(name, "ws-auth-user")) globals.auth_user = switch_core_strdup(pool, value);
				else if (!strcmp(name, "ws-auth-pass")) globals.auth_pass = switch_core_strdup(pool, value);
				else if (!strcmp(name, "ws-query-params")) globals.query = switch_core_strdup(pool, value);
				else if (!strcmp(name, "max-queue-size")) globals.max_queue_size = atoi(value);
				else if (!strcmp(name, "drop-threshold")) globals.drop_threshold = atoi(value);
				else if (!strcmp(name, "reconnect-interval")) globals.reconnect_interval = atoi(value);
				else if (!strcmp(name, "max-retry-count")) { globals.max_retry_count = atoi(value); if (globals.max_retry_count < 1) globals.max_retry_count = 1; }
				else if (!strcmp(name, "bypass-recovery-interval")) { globals.bypass_recovery_sec = atoi(value); if (globals.bypass_recovery_sec < 0) globals.bypass_recovery_sec = 0; }
			}
		}
		switch_xml_free(xml);
	}
	globals.config_pool = pool;
	return SWITCH_STATUS_SUCCESS;
}

SWITCH_MODULE_LOAD_FUNCTION(mod_ws_media_load)
{
	switch_api_interface_t *api_interface;
	switch_application_interface_t *app_interface;

	*module_interface = switch_loadable_module_create_module_interface(pool, modname);
	if (load_config(SWITCH_FALSE) != SWITCH_STATUS_SUCCESS) return SWITCH_STATUS_FALSE;

	SWITCH_ADD_API(api_interface, "uuid_ws_media", "WebSocket media tap", ws_media_api,
		"<uuid> start [ws-url] [in=read|write|mixed|stereo] [role=<label>] [call_id=<id>] | <uuid> stop");
	SWITCH_ADD_APP(app_interface, "ws_media_start", "Start WebSocket media tap", "Start WebSocket media tap",
		ws_media_start_app, "[ws-url] [in=read|write|mixed|stereo] [role=<label>] [call_id=<id>]", SAF_NONE);
	SWITCH_ADD_APP(app_interface, "ws_media_stop", "Stop WebSocket media tap", "Stop WebSocket media tap",
		ws_media_stop_app, "", SAF_NONE);

	/* fs_cli TAB completion: complete the uuid from active calls, then the verb
	 * and the capture-mode option. (URL/role/call_id are free-form and can't be
	 * completed positionally.) */
	switch_console_set_complete("add uuid_ws_media ::console::list_uuid start");
	switch_console_set_complete("add uuid_ws_media ::console::list_uuid start in=read");
	switch_console_set_complete("add uuid_ws_media ::console::list_uuid start in=write");
	switch_console_set_complete("add uuid_ws_media ::console::list_uuid start in=mixed");
	switch_console_set_complete("add uuid_ws_media ::console::list_uuid start in=stereo");
	switch_console_set_complete("add uuid_ws_media ::console::list_uuid stop");

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_ws_media (v1 tap) loaded\n");
	return SWITCH_STATUS_SUCCESS;
}

SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_ws_media_shutdown)
{
	switch_console_set_complete("del uuid_ws_media");
	if (globals.config_pool) switch_core_destroy_memory_pool(&globals.config_pool);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_ws_media unloaded\n");
	return SWITCH_STATUS_SUCCESS;
}
