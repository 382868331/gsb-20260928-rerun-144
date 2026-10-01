# 多人会话组织服务

Go 实现的本地多人房间组织服务。服务使用 TCP + 换行分隔 JSON，内部把连接、排队、取消、确认和定时器全部串行到一个事件循环，保证匹配与取消同时发生时结果确定。

## 功能

- 玩家先握手获得服务端签发的会话 ID，旧连接不能代替新会话确认。
- 等待队列按 FIFO 选择；同模式、同目标人数、等级差不超过 2 才可组队。
- 支持 2-4 人房间，首轮邀请需要全员确认。
- 玩家已在房间或队列中时不能重复占位。
- 掉线、拒绝或取消会释放名额；有合格等待者时立即补位并推送新邀请。
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

演练会建立真实 TCP 客户端，覆盖请求拆包、多请求合并到达、匹配、全员确认、拒绝、排队取消、掉线补位、旧房间邀请失效、保存后重启恢复和服务端关闭唤醒。当前容器若禁止绑定端口，自动化测试仍可通过，演示需在允许本地监听的环境运行。

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

响应类型包括 `queued`、`invited`、`confirmed`、`room_state`、`resumed`、`canceled`、`rejected` 和 `error`。`room_state` 在房间成员或等待状态变化时推送给当前有效会话。
