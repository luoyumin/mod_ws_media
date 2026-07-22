# mod_ws_media 发包协议说明

本文档描述 `mod_ws_media` 当前使用的 WebSocket 传输协议。模块本身是 WebSocket 客户端，外部音频处理服务是 WebSocket 服务端。

## 传输层

- 协议：WebSocket over TCP。
- 地址：由 `ws-host`、`ws-port`、`ws-path`、`ws-query-params` 配置决定。
- TLS：`ws-ssl=true` 时启用。
- 认证：可选 HTTP Basic Auth，通过 `ws-auth-user` 和 `ws-auth-pass` 配置。
- WebSocket 版本：RFC 6455 / version 13。
- 客户端发给服务端的 WebSocket frame 会按 RFC 6455 要求进行 mask。
- 服务端发给客户端的 frame 正常不应 mask。当前客户端可以解 masked server frame，但服务端不要依赖这个兼容行为。

每通电话会建立两条独立 WebSocket 连接：

| 连接 | init 中的 `direction` | 采集的音频来源 | 服务端返回音频注入到 |
|---|---|---|---|
| READ | `read` | B 侧音频 | A 侧播放 |
| WRITE | `write` | A 侧音频 | B 侧播放 |

方向只通过连接初始化包识别。后续 binary 音频帧里没有方向字段，也没有自定义 header。

## 连接流程

每个方向的连接流程如下：

1. TCP 连接到配置的 host 和 port。
2. 如果启用 TLS，执行 TLS handshake。
3. 执行 WebSocket HTTP upgrade handshake。
4. 客户端发送一个 text init frame。
5. 客户端开始发送 binary PCM 音频帧。
6. 服务端可以在同一条连接上返回 binary PCM 音频帧。

服务端必须把 READ 和 WRITE 连接当作两路独立音频流处理。

## Init 文本帧

WebSocket handshake 成功后，客户端立即发送一个 JSON text frame：

```json
{
  "type": "init",
  "uuid": "call-uuid",
  "direction": "read",
  "encoding": "L16",
  "sample_rate": 8000,
  "channels": 1,
  "ptime": 20,
  "bytes_per_frame": 320,
  "channel_codec": "PCMU"
}
```

字段说明：

| 字段 | 类型 | 含义 |
|---|---:|---|
| `type` | string | 固定为 `init`。 |
| `uuid` | string | FreeSWITCH call/session UUID。 |
| `direction` | string | `read` 或 `write`。 |
| `encoding` | string | 当前实现固定为 `L16`。 |
| `sample_rate` | number | 解码后的 PCM 采样率，来自 `actual_samples_per_second`。 |
| `channels` | number | PCM 声道数，通常为 `1`。 |
| `ptime` | number | 每包音频时长，单位毫秒。 |
| `bytes_per_frame` | number | FreeSWITCH 解码后一帧音频的名义字节数。 |
| `channel_codec` | string | 原始通道 codec 名称，仅作信息参考。 |

音频 payload 始终是 raw signed 16-bit little-endian PCM，也就是 `L16`。即使 SIP/RTP 侧 codec 是 PCMU、PCMA、G.722、Opus 等，传给服务端的也不是压缩 RTP payload，而是 FreeSWITCH media bug 看到的解码后 PCM。

服务端可以返回一个 text frame 作为确认，例如：

```json
{
  "type": "init_ack",
  "status": "ready"
}
```

这个 ack 是可选的。客户端会忽略非 binary 的 text frame，所以 ack 主要用于服务端日志和调试。

## 客户端到服务端的音频帧

音频通过 WebSocket binary frame 发送：

- Opcode：`0x2`。
- Payload：raw signed 16-bit little-endian PCM。
- 没有模块自定义 header。
- 没有方向字节或方向标记。
- 方向由当前连接的 init `direction` 决定。

重要约束：一个 WebSocket binary message 不保证等于一帧 FreeSWITCH 音频。

发送线程会读取当前方向缓冲区里的所有可用字节并作为一个 binary message 发送。因此一个 binary message 可能是：

- 正好一帧解码音频；
- 多帧解码音频拼在一起；
- 队列压力下丢弃旧音频后剩余的一段数据。

因此，服务端可以用 `bytes_per_frame` 做校验、日志和切帧参考，但不能把它当成每个 WebSocket message 的固定长度。

如果服务端只是录音写 WAV，可以把收到的 binary payload 按到达顺序直接写入 WAV data 区。WAV 参数使用：

- sample rate：`sample_rate`
- channels：`channels`
- bits per sample：`16`
- format：PCM signed little-endian

## 服务端到客户端的音频帧

服务端返回给模块的音频也必须是 WebSocket binary frame：

- Opcode：`0x2`。
- Payload：raw signed 16-bit little-endian PCM。
- 采样率和声道数应与 init 中声明的一致。
- 没有自定义方向 header。

串行模式下，服务端返回的 binary 音频会被写入对应方向的接收缓冲区，并注入回通话：

| init `direction` | 服务端返回音频播放给 |
|---|---|
| `read` | A 侧 |
| `write` | B 侧 |

并行模式下，模块只把通话音频复制给服务端，原始通话音频不被修改。当前实现仍然会启动接收线程。如果服务端完全不返回 frame，客户端的 recv 超时会被视为连接异常，可能触发重连或 bypass。对于模拟服务端或只录音的服务端，最简单稳定的方式是把收到的 binary 音频原样 echo 回同一条连接。

## 控制帧和限制

服务端 frame 支持情况：

| Frame | 行为 |
|---|---|
| Binary `0x2` | 作为音频接收。 |
| Text `0x1` | debug 日志记录后忽略。 |
| Ping `0x9` | 客户端自动回复 Pong `0xA`。 |
| Pong `0xA` | 忽略。 |
| Close `0x8` | 视为连接关闭。 |

当前限制：

- 不支持 fragmented WebSocket frame。
- RSV bits 不能被设置。
- 服务端单个 frame payload 上限是 `1 MiB`。
- control frame payload 必须 `<= 125` bytes。

## 缓冲和丢包

每通电话有四个音频缓冲区：

- READ send buffer
- READ recv buffer
- WRITE send buffer
- WRITE recv buffer

相关配置：

| 配置 | 含义 |
|---|---|
| `max-queue-size` | 队列超过该字节数后触发丢弃。 |
| `drop-threshold` | 丢弃旧音频后希望保留到的目标大小。 |
| `packet-loss-threshold` | 丢包率超过该阈值后进入 bypass。 |

模块优先保证低延迟，而不是保证音频完整性。当队列过大时，会丢弃旧音频。

## 重连和 Bypass

如果接收失败、recv 超时或 WebSocket 关闭：

1. 当前方向连接会被断开。
2. 等待 `reconnect-interval` 秒。
3. 尝试重连该方向。
4. 如果重试次数超过 `max-retry-count`，进入 bypass mode。

bypass mode 下，通话音频继续由 FreeSWITCH 原样通过，模块停止 WebSocket 音频处理。

## 服务端最小实现要求

一个兼容当前模块的服务端至少需要做到：

1. 在配置路径上接受 WebSocket 连接，例如 `/media`。
2. 读取第一条 text frame，解析 init JSON。
3. 每条连接维护一个独立 stream state。
4. 每收到一条 binary message：
   - 如果是录音服务，把 payload 追加写入对应 WAV/PCM 文件；
   - 如果需要支持 serial 模式或稳定模拟服务，把 binary PCM 按同样格式返回到同一连接。
5. init 后的 text frame 可以忽略或只做日志。
6. WebSocket 关闭时，正确回填 WAV 的 RIFF/data size 并关闭文件。

## 录音文件映射建议

例如 call UUID 为 `abc`：

| 连接 | 建议 WAV 文件名 |
|---|---|
| `direction=read` | `abc-read.wav` |
| `direction=write` | `abc-write.wav` |

如果需要单个混音录音文件，建议服务端后处理时再把 `read` 和 `write` 两路 WAV 混音。当前模块协议是两条连接分别传输两路方向音频，不会把双向音频复用到同一条 WebSocket 连接里。
