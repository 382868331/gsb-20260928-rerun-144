# 多人会话组织服务 (sessionorg)

Go 实现的首轮多人会话组织服务：玩家连接、等待队列、房间生命周期、按条件组队与确认加入。
后续可在 room_confirmed 事件处对接实际游戏服务。

## 运行

    go run . -addr 127.0.0.1:7777 -data data -room-size 2 -confirm-timeout 5s

协议为 TCP 上的换行分隔 JSON。

## 客户端消息

- hello     {player_id}          绑定玩家；新连接取代旧会话
- enqueue   {mode}               按条件(mode)进入等待队列
- cancel                         取消排队
- confirm     {room_id, token}      确认加入匹配到的房间
- leave_room                     离开已确认房间，释放名额
- status                         查询自身状态

## 服务端推送

- welcome / queued / cancelled / cancel_failed
- match_offer {room_id, token, players}  组队成功，待确认
- confirm_ok / confirm_failed
- room_confirmed {room_id, players}      全员确认，房间生命周期开始
- match_aborted {room_id, reason}        有人掉线或确认超时，名额已释放
- error {reason}

## 关键语义

- 单事件循环串行处理：取消与匹配同时发生时结果确定（先到先生效）。
- 已确认房间的玩家再 enqueue 会被拒绝，不能同时占两个已确认房间。
- 匹配后有人掉线或确认超时：房间解散、名额释放，仍在等待的玩家收到 match_aborted 并回到队首，可立即重新组队。
- confirm 令牌绑定签发时的会话；旧连接（被取代的会话）不能替新会话确认。
- 关键状态原子写入 data/state.json；重启后已确认房间保留（玩家重连恢复），待确认房间解散、队列清空，不存在重复占位或永远卡住的玩家。启动与每次事件后都校验不变量（checkInvariants）。

## 测试（本地虚拟客户端）

    go test -race -count=1 -v ./...

sim_test.go 用内存管道虚拟客户端覆盖：条件组队与确认、禁止双房间、掉线释放并通知、取消/匹配竞争、旧会话确认拒绝、确认超时、重启持久化核对。
