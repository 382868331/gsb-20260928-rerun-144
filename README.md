# 多人会话组织服务

Go 实现的本地多人房间组织服务。服务使用 TCP + 换行分隔 JSON，内部把连接、排队、取消、确认和定时器全部串行到一个事件循环，保证匹配与取消同时发生时结果确定。

## 功能

- 玩家先握手获得服务端签发的会话 ID，旧连接不能代替新会话确认。
- 等待队列按 FIFO 选择；同模式、同目标人数、等级差不超过 2 才可组队。
- 支持 2-4 人房间，首轮邀请需要全员确认。
- 最后一份确认到达后房间进入 active，服务端把最终决定广播给全部成员；重复确认幂等返回当前状态，不会让房间停在旧阶段。
- 玩家已在房间或队列中时不能重复占位。
- 已确认成员掉线后席位保留一段时间（默认 5 秒，demo 可用 -reserve-ttl 调整），保留期内重连可回到原席位；保留到期才释放名额并补位，边界明确。
- 未确认成员掉线、拒绝或取消会立即释放名额；有合格等待者时立即补位并推送新邀请。
- 旧连接的迟到退出或确认因会话失效被拒绝，不能作用于新接入的身份。
- 已完成的房间若确认成员全部离开，房间直接解散并重新排队，不会被补位流程重新打开。
- 10 秒未确认会释放对应成员并重新排队；活跃房间缺员且 15 秒无法补齐时回收房间并重新排队。
- 关键状态通过临时文件加原子重命名保存为 JSON；重启后校验重复占位，并恢复活跃房间。

## 运行

```bash
go test ./...
go run ./cmd/server -addr 127.0.0.1:7700 -state ./state.json
```

本地真实 TCP 客户端演练：

```bash
go run ./cmd/demo -state /tmp/multiplayer-session-demo.json
```

演练会建立真实 TCP 客户端，并主动制造请求字节分片和两条请求合并到达，依次展示：匹配与全员确认后的状态收敛（含重复确认）、短暂断线后的席位保留与重连回位、保留到期后的等待者补位、旧会话迟到确认被拒绝、确认成员全部离开后房间解散不重用，以及保存后重启恢复并解释最终决定。每个客户端连接持续复用同一个带缓冲的换行帧读取器，避免合并到达的下一条报文被丢弃。

`internal/server_test.go` 中的 TCP 回归测试会在允许本地监听的环境强制执行；如果容器内核返回 `socket: operation not permitted`，这些测试会跳过，纯引擎和持久化回归仍会正常执行。

## 协议

每条消息是一行 JSON。客户端首先发送：

```json
{"type":"hello","player_id":"p1"}
```

服务端返回 `hello` 和 `session_id`。后续请求都携带该 ID：

```json
{"type":"queue","session_id":"...","player_id":"p1","mode":"ranked","level":10,"room_size":2}
{"type":"confirm","session_id":"...","player_id":"p1","room_id":"room-1"}
{"type":"reject","session_id":"...","player_id":"p1","room_id":"room-1"}
{"type":"cancel","session_id":"...","player_id":"p1"}
{"type":"snapshot","session_id":"...","player_id":"p1"}
```

响应类型包括 `queued`、`invited`、`confirmed`、`room_state`、`resumed`、`canceled`、`rejected` 和 `error`。`room_state` 在房间成员或等待状态变化时推送给当前有效会话，携带房间状态（`status`）、已确认成员（`members`）与保留中的席位（`reserved`）；房间进入 active 时服务端会向全部成员广播最终决定。
