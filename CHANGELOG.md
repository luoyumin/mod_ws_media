# Changelog

Notable changes per release. Releases before v1.2.0 are described in the
[GitHub Releases](https://github.com/luoyumin/mod_ws_media/releases) notes.

## v1.2.0

Capture and teardown correctness, plus the observability needed to catch this
class of problem next time. No wire-protocol changes: a server written against
v1.1.0 keeps working.

> ### Upgrade strongly recommended if you use `in=read` or `in=write`
>
> **Affected: every release up to and including v1.1.0** (v1.0.0, v1.0.1,
> v1.0.2, v1.1.0) — the capture loop below is present in all of them.
>
> **`in=read` is the default**, so this applies to any attach that did not pass
> `in=` explicitly.
>
> On a single-direction tap, from the moment the tap connects:
>
> - the leg **cannot be hung up** — `uuid_kill` / `hupall` have no effect,
>   `ws_cleanup()` never runs (so the peer never gets the `stop` frame or the
>   Close either), and the channel is only cleared by restarting FreeSWITCH;
> - the stream carries **fabricated `0xFF` filler** at many times realtime,
>   while most of the real captured audio is dropped at `max-queue-size`.
>
> **Not affected: `in=stereo` and `in=mixed`** — they set both stream flags, so
> the loop terminates normally. This is why the existing soak suite, which runs
> stereo, never caught it.
>
> A second, milder issue also affects all those releases: hanging up while the
> backend was unreachable stalled channel teardown for up to
> `reconnect-interval` seconds. Details in *Fixed* below.

### Fixed

- **Capture read one frame per callback instead of draining in a loop.** The
  loop had two consequences, both of them serious.

  `switch_core_media_bug_read()` only reports "nothing to give" when *both* the
  read and write buffers are dry. On a single-direction tap (`in=read` or
  `in=write`, where only one `SMBF_*_STREAM` flag is set) that condition is
  unreachable, so repeated calls inside one callback keep returning success with
  synthesised `0xFF` filler frames.

  1. **The channel could wedge permanently.** The callback runs on the leg's own
     session thread, which is also the only thread that can carry that leg
     through hangup. An endless loop there means `hupall` has no effect and the
     channel is only cleared by restarting FreeSWITCH.
  2. **Far more audio was sent than the call produced.** Measured against
     `uuid_record` on the same leg at the same time: ~13x realtime, with 87% of
     captured frames dropped at `max-queue-size`. After the fix both agree at
     1.0x with zero drops.

  `in=stereo` and `in=mixed` set both stream flags and were unaffected, which is
  why the existing soak suite (stereo) never caught it.

- **Reconnect backoff waits in 100ms slices and re-checks the channel.** A
  single `switch_yield(reconnect-interval)` cannot be woken, and teardown joins
  the reconnect thread — so hanging up while the backend was unreachable stalled
  channel teardown for the whole interval (measured 3920ms, now ~60ms). With the
  default settings roughly a third of the time during a sustained outage fell in
  that window.

- **Re-check `switch_channel_up()` before dialling.** A leg that hung up during
  the backoff no longer gets a connection and a `start` frame for a call that no
  longer exists.

- **`ws_cleanup()` lifecycle.** `send_buffer` is now destroyed under
  `audio_mutex`, and the channel's private handle on the bug is cleared on the
  hangup path too (previously only `uuid_ws_media stop` cleared it). Both were
  reachable as use-after-free once `stats` started reading that state.

- **Config values are validated at load.** `ws-port`, `max-queue-size`,
  `drop-threshold` and `reconnect-interval` are range-checked, with a warning
  naming the value that was overridden — `atoi()` silently turns anything
  unparsable into 0. `drop-threshold` matters most: at or above
  `max-queue-size` the send thread's `inuse - drop-threshold` underflows in
  `switch_size_t` and tosses the entire buffer, losing audio with nothing in the
  log.

### Added

- **`uuid_ws_media <uuid> stats`** — state, uptime, retry count, frames sent,
  frames dropped, bytes sent, queued bytes. This is the `stats` command
  described in `docs/DESIGN.md` §14.
- **Counters on every `ws_media::*` event** — `Frames-Sent`, `Frames-Dropped`,
  `Bytes-Sent`, so a consumer can reconcile a stream without polling.
- **Session-scoped logging.** Module log lines now carry the channel UUID, so a
  backend problem can be attributed to a specific call instead of producing a
  screen of identical, unattributable errors.

### Known gaps

- `ws_connect()` is not interruptible: `ws_open_socket()` keeps the fd in a local
  until connect succeeds, so the `shutdown()` that wakes a blocked reader is a
  no-op for the whole attempt. A hangup landing mid-connect waits out DNS plus
  one connect timeout per resolved address plus the handshake read timeouts
  (measured 1620ms against an unroutable address). It is a delay, not a wedge —
  teardown always completes. See `docs/DESIGN.md` §11.
