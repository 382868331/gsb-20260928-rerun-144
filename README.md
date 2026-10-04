# 多人会话组织服务

Go 实现的本地多人房间组织服务。服务使用 TCP + 换行分隔 JSON，内部把连接、排队、取消、确认和定时器全部串行到一个事件循环，保证匹配与取消同时发生时结果确定。

## 功能

- 玩家先握手获得服务端签发的会话 ID，旧连接不能代替新会话确认。
- 等待队列按 FIFO 选择；同模式、同目标人数、等级差不超过 2 才可组队。
- 支持 2-4 人房间，首轮邀请需要全员确认。
- 玩家已在房间或队列中时不能重复占位。
- 最后一次确认让房间进入 active，所有成员都会收到同一个 confirmed 最终决定；重复确认幂等返回当前状态，不会让房间退回提议阶段。
- 已确认成员短暂掉线时席位保留 5 秒，保留期内不补位；重新握手即可回到原席位，旧连接的迟到退出或确认不会作用到新会话。
- 保留到期仍未归来的席位才会释放并立即补位；未确认成员的掉线、拒绝或取消会立刻释放名额。
- 已取消或已完成的房间不会被补位流程重新打开。
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

演练会建立真实 TCP 客户端，并主动制造请求字节分片和两条请求合并到达，依次核对：最后确认后全员收敛到同一个 active 决定、重复确认幂等、短暂断线保留席位并重连恢复、旧会话确认被拒绝、保留到期后等待者补位、迟到确认不再影响房间、重启后通过 result 查询仍能解释最终决定、旧邀请无法确认重匹配房间，以及关闭时等待者收到取消结果。每个客户端连接持续复用同一个带缓冲的换行帧读取器，避免合并到达的下一条报文被丢弃。

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
{"type":"result","session_id":"...","player_id":"p1"}
```

响应类型包括 `queued`、`invited`、`confirmed`、`room_state`、`resumed`、`canceled`、`rejected`、`result` 和 `error`。未完成的确认返回 `room_state`；房间达到 active 时，所有成员都会收到携带相同成员名单的 `confirmed`。成员断线、保留到期、补位邀请或等待队列变化时，当前有效会话都会收到 `room_state`。

房间相关响应携带稳定的 `seats` 席位数组（按席位号排序，长度等于目标人数），每个席位包含：

- `status=confirmed`：已确认席位；`online=true` 表示当前会话在线，`false` 不会出现在已确认在线成员身上。
- `status=reserved`：已确认成员断线后的保留席位，`deadline_ms` 是保留截止时间；此前不会补位，也不会出现空缺。
- `status=invited`：席位已被补位邀请占用但尚未确认，`deadline_ms` 是该邀请的提议截止时间。
- `status=vacant`：保留到期后释放、等待补位的空缺席位。

`members` 仍只列出当前已确认的成员 ID，客户端无需通过成员是否在名单中猜测断线；归属和保留/补位边界一律以 `seats` 为准。最终确认或补位确认使房间全员确认后，活动房间残留的提议 `deadline_ms` 会立即清零，并从持久化 JSON 中省略；保留截止时间则始终按断线时刻持久化，保存重开后沿用同一个到期边界。重开时没有活动会话的席位按保留席位处理。
