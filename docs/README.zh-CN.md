# mod_ws_media - WebSocket Media Bridge Module

## 概述

`mod_ws_media` 是一个通用的 FreeSWITCH 模块，用于在通话过程中通过 WebSocket 将语音流实时发送给第三方服务，并接收处理后的语音流注入到通话中。模块不关心服务端提供什么服务，可以用于翻译、语音识别、音频处理等任何场景。

## 特性

- **两种处理模式**:
  - **串行模式（Serial Mode）**: 媒体流经过 WebSocket 处理后替换原始流
  - **并行模式（Parallel Mode）**: 复制媒体流发送给 WebSocket，原始流不受影响
- **低延迟设计**: 通过丢包策略优先保证延迟
- **智能丢包监控**: 自动监控丢包率，超过阈值自动切换到直通模式
- **可插拔架构**: 最小化对 FreeSWITCH 源代码的侵入
- **智能重试**: 可配置的重试次数（默认3次）
- **直通模式**: 重试失败或丢包率过高时自动切换到直通模式
- **认证支持**: 支持 HTTP Basic Authentication
- **灵活配置**: 支持 WebSocket 查询参数和各种配置选项
- **ESL 事件**: 生成详细的事件供订阅者订阅

## 编译

在 FreeSWITCH 源码根目录执行：

```bash
make mod_ws_media
```

## 配置

编辑 `conf/autoload_configs/ws_media.conf.xml`：

```xml
<configuration name="ws_media.conf" description="WebSocket Media Bridge Module">
  <settings>
    <!-- WebSocket 服务器配置 -->
    <param name="ws-host" value="localhost"/>
    <param name="ws-port" value="8080"/>
    <param name="ws-path" value="/media"/>
    <param name="ws-ssl" value="false"/>

    <!-- 认证配置（可选） -->
    <!-- <param name="ws-auth-user" value="username"/> -->
    <!-- <param name="ws-auth-pass" value="password"/> -->

    <!-- WebSocket 握手查询参数（可选，示例: session_id=123&api_key=abc） -->
    <!-- <param name="ws-query-params" value="session_id=123&api_key=abc"/> -->

    <!-- 缓冲和延迟设置 -->
    <param name="max-queue-size" value="8192"/>
    <param name="drop-threshold" value="4096"/>
    <param name="reconnect-interval" value="5"/>

    <!-- 重试配置 -->
    <param name="max-retry-count" value="3"/>

    <!-- 丢包率阈值 (0.0-1.0，例如 0.3 = 30%) -->
    <param name="packet-loss-threshold" value="0.3"/>
  </settings>
</configuration>
```

### 配置参数说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `ws-host` | `localhost` | WebSocket 服务器主机名或 IP |
| `ws-port` | `8080` | WebSocket 服务器端口 |
| `ws-path` | `/media` | WebSocket 路径 |
| `ws-ssl` | `false` | 是否使用 SSL/TLS |
| `ws-auth-user` | - | HTTP Basic 认证用户名（可选） |
| `ws-auth-pass` | - | HTTP Basic 认证密码（可选） |
| `ws-query-params` | - | 握手时附加的 URL 查询参数（可选） |
| `max-queue-size` | `8192` | 最大队列大小（字节），超过此值将丢包 |
| `drop-threshold` | `4096` | 队列超过此值时开始丢包（字节） |
| `reconnect-interval` | `5` | 重连间隔（秒） |
| `max-retry-count` | `3` | 最大重试次数，超过后切换到直通模式 |
| `packet-loss-threshold` | `0.3` | 丢包率阈值（0.0-1.0），超过后切换到直通模式 |

## 使用方法

### 1. 加载模块

```
load mod_ws_media
```

或在 `autoload_configs/modules.conf.xml` 中添加：

```xml
<load module="mod_ws_media"/>
```

### 2. 设置处理模式

通过通道变量 `ws_media_mode` 控制处理模式：

#### 串行模式（Serial Mode，默认）

媒体流经过 WebSocket 处理后替换原始流：

```xml
<action application="set" data="ws_media_mode=serial"/>
<action application="ws_media_start"/>
```

#### 并行模式（Parallel Mode）

复制媒体流发送给 WebSocket，原始流不受影响：

```xml
<action application="set" data="ws_media_mode=parallel"/>
<action application="ws_media_start"/>
```

### 3. 通过 API 控制

```
fs_cli> uuid_ws_media <uuid> start
fs_cli> uuid_ws_media <uuid> stop
```

### 4. 停止处理

```xml
<action application="ws_media_stop"/>
```

## 重试机制和直通模式

### 重试机制

模块在以下情况会进行重试：
1. **初始连接失败**: 启动时如果连接失败，会立即重试，最多重试 `max-retry-count` 次
2. **连接断开**: 运行中如果连接断开，接收线程会尝试重连，最多重试 `max-retry-count` 次

### 直通模式（Bypass Mode）

当以下情况发生时，模块会自动切换到**直通模式**：
1. 重试次数达到 `max-retry-count`
2. 丢包率超过 `packet-loss-threshold`

直通模式下：
- 音频流直接桥接，不再通过 WebSocket 处理
- 通话可以正常进行，不受 WebSocket 服务影响
- 模块会生成 `ws_media::error` 事件
- Media Bug 仍然附加在通道上，可以通过 `ws_media_stop` 停止

## ESL 事件

| 事件 | 说明 |
|------|------|
| `ws_media::start` | 处理开始 |
| `ws_media::stop` | 处理停止 |
| `ws_media::connected` | WebSocket 连接成功（含 `Direction` 字段：READ/WRITE） |
| `ws_media::disconnected` | WebSocket 断开连接（含 `Direction` 字段） |
| `ws_media::error` | 发生错误（含 `Error` 字段说明原因） |
| `ws_media::audio_sent` | 音频已发送 |
| `ws_media::audio_received` | 音频已接收（含 `Direction` 字段） |

订阅示例：

```bash
fs_cli> /events plain CUSTOM ws_media::start ws_media::stop ws_media::error ws_media::connected
```

## 使用示例

### 示例 1：串行模式 - 实时翻译

A 和 B 通话，A 说中文，B 听到英文；B 说英文，A 听到中文：

```xml
<extension name="translation_call">
  <condition field="destination_number" expression="^9999$">
    <action application="set" data="ws_media_mode=serial"/>
    <action application="answer"/>
    <action application="ws_media_start"/>
    <action application="bridge" data="user/1000"/>
    <action application="ws_media_stop"/>
  </condition>
</extension>
```

### 示例 2：并行模式 - 通话录音/分析

A 和 B 正常通话，同时将媒体流发送给外部服务进行分析：

```xml
<extension name="monitored_call">
  <condition field="destination_number" expression="^8888$">
    <action application="set" data="ws_media_mode=parallel"/>
    <action application="answer"/>
    <action application="ws_media_start"/>
    <action application="bridge" data="user/1000"/>
    <action application="ws_media_stop"/>
  </condition>
</extension>
```

### 示例 3：通过 API 动态启动

```bash
# 设置模式并启动
fs_cli -x "uuid_setvar <uuid> ws_media_mode serial"
fs_cli -x "uuid_ws_media <uuid> start"

# 停止处理
fs_cli -x "uuid_ws_media <uuid> stop"
```

### 示例 4：originate 时指定模式

```bash
# 串行模式
fs_cli -x "originate {ws_media_mode=serial}user/1000 &echo"

# 并行模式
fs_cli -x "originate {ws_media_mode=parallel}user/1000 &echo"
```

## WebSocket 协议

### 支持的音频编解码器

模块通过 FreeSWITCH 的 Media Bug 机制工作。**Media Bug 回调中的音频数据永远是解码后的 raw 16-bit signed PCM（L16）**，与通道使用的 RTP 编解码器无关——FreeSWITCH 在交给 Media Bug 之前会自动完成解码。

因此模块**天然支持所有 FreeSWITCH 支持的音频编解码器**，包括但不限于：

| 编解码器 | 采样率 | 声道 | bytes_per_frame (20ms) |
|---------|--------|------|------------------------|
| PCMU (G.711 μ-law) | 8000 Hz | 1 | 320 |
| PCMA (G.711 A-law) | 8000 Hz | 1 | 320 |
| G.722 | 16000 Hz | 1 | 640 |
| Opus (窄带) | 8000 Hz | 1 | 320 |
| Opus (宽带) | 16000 Hz | 1 | 640 |
| Opus (全带) | 48000 Hz | 1 | 1920 |
| Opus (全带立体声) | 48000 Hz | 2 | 3840 |

> 第三方服务只需处理 raw L16 PCM 数据，无需了解原始 RTP 编解码器。

### 连接流程

1. 建立 TCP/SSL 连接
2. 发送 WebSocket 握手请求（包含认证头和查询参数）
3. 接收 `101 Switching Protocols` 响应
4. 模块发送 JSON 格式的初始化包（text frame）

### 初始化包

每次连接建立后，模块自动发送以下 JSON 文本帧，描述即将接收的 PCM 数据格式：

```json
{
  "type": "init",
  "uuid": "<channel-uuid>",
  "direction": "read|write",
  "encoding": "L16",
  "sample_rate": 8000,
  "channels": 1,
  "ptime": 20,
  "bytes_per_frame": 320,
  "channel_codec": "PCMU"
}
```

| 字段 | 说明 |
|------|------|
| `encoding` | 始终为 `"L16"`（有符号 16-bit 小端 PCM），即实际传输的数据格式 |
| `sample_rate` | 解码后的 PCM 采样率（Hz），使用实际采样率（G.722 为 16000 而非 8000） |
| `channels` | 声道数（1=单声道，2=立体声） |
| `ptime` | 每帧时长（毫秒） |
| `bytes_per_frame` | 每帧精确字节数，等于 `sample_rate × channels × ptime/1000 × 2` |
| `channel_codec` | 通道使用的 SIP/RTP 编解码器名称（仅供参考，不影响 PCM 数据格式） |

服务端可选择返回 `{"type":"init_ack","status":"ok"}` 文本帧确认（模块会自动忽略，不写入音频缓冲区）。

### 音频数据

初始化之后，WebSocket 连接上的所有帧均为**二进制帧**，携带 raw L16 PCM 数据：

- **格式**: 有符号 16-bit 小端整数
- **帧大小**: 每帧固定为 `bytes_per_frame` 字节（由 ptime 决定）
- **采样率/声道**: 由 init 包中的 `sample_rate` 和 `channels` 给出

串行模式下，服务端返回的二进制帧必须与输入帧格式完全一致（采样率、声道数、字节数均相同）。

### 认证

配置了 `ws-auth-user` 和 `ws-auth-pass` 时，握手请求中自动添加：

```
Authorization: Basic <base64(username:password)>
```

### 查询参数

配置了 `ws-query-params` 时，参数附加到握手 URL：

```
GET /media?session_id=123&api_key=abc HTTP/1.1
```

## 性能优化

### 处理模式选择

| 模式 | 优点 | 缺点 | 适用场景 |
|------|------|------|---------|
| 串行 | 可替换/修改音频流 | 增加延迟 | 实时翻译、降噪、语音转换 |
| 并行 | 不影响原始通话 | 无法修改音频 | 录音、监控、语音识别 |

### 低延迟策略

1. **丢包机制**: 队列超过 `drop-threshold` 时自动丢弃旧数据
2. **小缓冲区**: 使用较小的缓冲区减少延迟
3. **独立线程**: 每个方向的发送/接收各有独立线程，互不阻塞
4. **自动监控**: 每 5 秒统计一次丢包率，超过阈值自动切换直通模式

## 故障排除

### Failed to add media bug (status=9)

通道没有建立媒体流。确认：
- 不使用 `park`（park 不建立媒体路径），改用 `echo`、`playback` 或 `bridge`
- `bypass_media=false`
- `proxy_media=false`

用诊断脚本检查通道状态：

```bash
./check_channel.sh <uuid>
```

### WebSocket 连接失败

1. 确认服务器正在运行并可达
2. 检查 `ws-host`、`ws-port` 配置
3. 查看 FreeSWITCH 日志：`fs_cli> /log 7`

### 音频质量问题

1. 检查网络延迟
2. 适当增大 `max-queue-size` 和 `drop-threshold`
3. 确认服务端返回的 PCM 格式与原始流匹配（采样率、位深）

## 架构说明

### 模块结构

```
mod_ws_media/
├── mod_ws_media.c                     # 主模块代码
├── Makefile.am                        # 构建配置
├── Makefile                           # 编译入口
├── conf/autoload_configs/
│   └── ws_media.conf.xml              # 配置文件
├── test_ws_server.py                  # 测试用 WebSocket echo 服务器
├── check_channel.sh                   # 通道状态诊断脚本
├── install_manual.sh                  # 本地开发环境安装脚本
└── README.md
```

### 核心组件

1. **Media Bug**: 拦截音频流（串行和并行模式都使用 `SMBF_READ_REPLACE|SMBF_WRITE_REPLACE`；并行模式只复制帧，不修改输出帧）
2. **WebSocket 客户端**: 手动实现 RFC 6455，含掩码、握手、帧解析
3. **发送线程 × 2**: `read_send_thread`（B→WebSocket）、`write_send_thread`（A→WebSocket）
4. **接收线程 × 2**: `read_recv_thread`（WebSocket→A）、`write_recv_thread`（WebSocket→B）
5. **事件系统**: 生成 ESL Custom 事件

详细发包规则见 [`PACKET_PROTOCOL.md`](PACKET_PROTOCOL.md)。

### 数据流

**串行模式（Serial Mode）- 双向处理：**

```
A说话 → WRITE_REPLACE → 发送缓冲区 → write_send_thread → WRITE WebSocket → 第三方服务
                                                                                    ↓
B听到 ← WRITE_REPLACE ← 接收缓冲区 ← write_recv_thread ← WRITE WebSocket ← 第三方服务

B说话 → READ_REPLACE  → 发送缓冲区 → read_send_thread  → READ WebSocket  → 第三方服务
                                                                                    ↓
A听到 ← READ_REPLACE  ← 接收缓冲区 ← read_recv_thread  ← READ WebSocket  ← 第三方服务
```

**并行模式（Parallel Mode）- 双向复制：**

```
A说话 → WRITE_STREAM → 发送缓冲区 → write_send_thread → WRITE WebSocket → 第三方服务
  ↓                                                                        （仅分析，不返回）
B听到（原始音频，未修改）

B说话 → READ_STREAM  → 发送缓冲区 → read_send_thread  → READ WebSocket  → 第三方服务
  ↓                                                                        （仅分析，不返回）
A听到（原始音频，未修改）
```

**直通模式（Bypass Mode）：**

```
A说话 → Media Bug → 直接通过 → B听到
B说话 → Media Bug → 直接通过 → A听到
```

### 连接架构

```
FreeSWITCH                        第三方服务
    │                                  │
    ├─── READ WebSocket 连接 ─────────>│  （接收/处理 B 的音频）
    │<── 返回处理后的音频 ─────────────┤
    │                                  │
    └─── WRITE WebSocket 连接 ────────>│  （接收/处理 A 的音频）
         <── 返回处理后的音频 ─────────┘
```

## 本地开发环境安装

针对源码目录 `freeswitch` + 安装目录 `fs_dev` 的布局：

```bash
# 编译
cd /path/to/freeswitch
make mod_ws_media

# 安装（将 .so 和配置文件复制到 fs_dev）
cd src/mod/applications/mod_ws_media
sudo ./install_manual.sh

# 加载模块
fs_dev/bin/fs_cli -x "load mod_ws_media"
```

如果 `.libs/` 目录权限为 root，需先清除旧编译产物：

```bash
sudo rm -rf src/mod/applications/mod_ws_media/.libs \
            src/mod/applications/mod_ws_media/mod_ws_media.la
```

## 测试

### 启动测试 WebSocket 服务器

```bash
pip3 install websockets
python3 test_ws_server.py
```

测试服务器会监听 `0.0.0.0:8080`，回显收到的音频数据，并打印初始化包信息。

### 拨号计划测试扩展

```xml
<!-- 串行模式测试：拨打 9901 -->
<extension name="ws_media_serial_test">
  <condition field="destination_number" expression="^9901$">
    <action application="answer"/>
    <action application="set" data="ws_media_mode=serial"/>
    <action application="ws_media_start"/>
    <action application="echo"/>
    <action application="ws_media_stop"/>
    <action application="hangup"/>
  </condition>
</extension>

<!-- 并行模式测试：拨打 9902 -->
<extension name="ws_media_parallel_test">
  <condition field="destination_number" expression="^9902$">
    <action application="answer"/>
    <action application="set" data="ws_media_mode=parallel"/>
    <action application="ws_media_start"/>
    <action application="echo"/>
    <action application="ws_media_stop"/>
    <action application="hangup"/>
  </condition>
</extension>
```

重新加载拨号计划：`fs_cli -x "reloadxml"`

### 注意：不能用 park

`park` 不会建立双向媒体流，Media Bug 无法附加。必须使用 `echo`、`playback`、`bridge` 等会真正建立媒体路径的应用。

通过 API 触发时使用 `echo`：

```bash
originate {ws_media_mode=parallel,bypass_media=false}user/1020 &echo
# 等待接听后
uuid_ws_media <uuid> start
```

## 限制

- 数据格式固定为 raw L16 PCM，第三方服务需自行处理重采样（如需将 8kHz 升至 16kHz）
- WebSocket 连接断开时会自动重连，但可能丢失部分音频
- 丢包策略可能导致音频质量下降
- 串行模式会增加通话延迟（取决于网络和处理速度）
- 并行模式下，第三方服务返回的数据会被忽略

## 未来改进

- [ ] 支持更多音频格式（Opus、G.711等）
- [ ] 添加统计信息 API（`uuid_ws_media <uuid> stats`）
- [ ] 支持配置热重载
- [ ] 支持 WebSocket 子协议
- [ ] 支持自定义 HTTP 头
- [ ] 支持单个 WebSocket 连接处理双向音频（全双工模式）

## 许可证

MPL 1.1
