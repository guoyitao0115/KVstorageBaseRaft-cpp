# KVstorageBaseRaft-cpp 学习日志

> 本文档用于记录我在学习本项目过程中的笔记、关键函数解读、疑问与回顾。
> 配套阅读：[`PROJECT_GUIDE.md`](./PROJECT_GUIDE.md)（项目全景 + 架构图 + 学习路线）。

## 目录

- [使用说明](#使用说明)
- [学习日志](#学习日志)
  - [2026-05-22 · `Raft::init` 节点初始化入口](#2026-05-22--raftinit-节点初始化入口)
  - [2026-05-23 · `Raft::electionTimeOutTicker` 选举超时看门狗](#2026-05-23--raftelectiontimeoutticker-选举超时看门狗)
  - [2026-05-23 · `Raft::doElection` 发起选举 + 五个深入问题](#2026-05-23--raftdoelection-发起选举--五个深入问题)
  - [2026-05-23 · `Raft::sendRequestVote` 投票回复处理](#2026-05-23--raftsendrequestvote-投票回复处理)
  - [2026-05-24 · `Raft::RequestVote` 投票决策（被动方）+ 安全性证明 + 概念辨析](#2026-05-24--raftrequestvote-投票决策被动方-安全性证明--概念辨析)
  - [2026-05-28 · `Raft::leaderHearBeatTicker` 心跳闹钟](#2026-05-28--raftleaderhearbeatticker-心跳闹钟)
  - [2026-05-29 · `Raft::doHeartBeat` 广播 AE / 日志复制 + 七个深入问题](#2026-05-29--raftdoheartbeat-广播-ae--日志复制--七个深入问题)
  - [2026-05-30 · `Raft::sendAppendEntries` 大集成 + 基础概念梳理 + Figure 8 反例](#2026-05-30--raftsendappendentries-大集成--基础概念梳理--figure-8-反例)
  - [2026-06-01 · `Raft::AppendEntries1` 被动方处理 AE + 跳跃式回退原理](#2026-06-01--raftappendentries1-被动方处理-ae--跳跃式回退原理)
  - [2026-06-07 · `Persister` 持久化文件读写器](#2026-06-07--persister-持久化文件读写器)
  - [2026-06-07 · `raft.cpp` 持久化链路总览](#2026-06-07--raftcpp-持久化链路总览)

---

## 使用说明

每次学习时按以下模板新建一节：

```markdown
### YYYY-MM-DD · 主题（关键函数 / 模块）

- **文件**：`相对路径`
- **行号**：起始行 ~ 结束行
- **关键词**：xxx / yyy

#### 代码位置
> 简述这是什么、被谁调用

#### 解读
> 分块讲解 + 关键点

#### 疑问 / TODO
> 还没想通的点
```

---

## 学习日志

### 2026-05-22 · `Raft::init` 节点初始化入口

- **文件**：`src/raftCore/raft.cpp`
- **行号**：988 ~ 1047
- **关键词**：Raft 启动 / 持久化恢复 / 三大 ticker / 协程 vs 线程

#### 代码位置

```cpp
void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers,
                int me,
                std::shared_ptr<Persister> persister,
                std::shared_ptr<LockQueue<ApplyMsg>> applyCh);
```

由 `KvServer` 在构造时调用，是 Raft 节点的**启动入口**，做三件事：装配状态、恢复持久化数据、启动后台循环。

#### 解读

**1) 四件套参数（988-989）**

| 参数 | 含义 |
| --- | --- |
| `peers` | 到其他节点的 RPC 客户端 stub 数组 |
| `me` | 当前节点在集群中的下标 / id |
| `persister` | 持久化器，落盘 `raftstatePersist<i>.txt`、`snapshotPersist<i>.txt` |
| `applyCh` | 与 `KvServer` 共享的 `LockQueue<ApplyMsg>`，committed 日志推送通道 |

**2) 初始化易失状态（994-1013）**

- 全部清零，对应论文图 2 "Volatile state on all servers"。
- `m_status = Follower`：所有节点冷启动一律 Follower。
- `m_votedFor = -1` 表示尚未投票。
- `m_nextIndex / m_matchIndex` 长度 = peer 数量；只有成为 Leader 才会被赋有效值。
- `m_lastResetElectionTime / m_lastResetHearBeatTime` 用 `now()` 作为超时判定基准。
- 加锁 `m_mtx` 是为了保持锁顺序的一致性，便于以后并发安全。

**3) 从持久化恢复（1015-1023）**

```cpp
readPersist(m_persister->ReadRaftState());
if (m_lastSnapshotIncludeIndex > 0) {
  m_lastApplied = m_lastSnapshotIncludeIndex;
}
```

- `readPersist` 反序列化覆盖：`currentTerm / votedFor / logs / lastSnapshotIncludeIndex / lastSnapshotIncludeTerm`。
- 如有快照，把 `lastApplied` 推到快照点，**避免重启后重复 apply**。
- TODO（源码注释）："崩溃恢复为何不能读取 commitIndex"。理论上可以一并把 `commitIndex` 设为 `lastSnapshotIncludeIndex`；当前没设也不会出错 —— 因为重启后 leader 的 AppendEntries 会很快把 `commitIndex` 抬上来，且 `lastApplied` 已被保护，不会重复 apply。

**4) 启动 IOManager（1027）**

```cpp
m_ioManager = std::make_unique<monsoon::IOManager>(FIBER_THREAD_NUM,
                                                   FIBER_USE_CALLER_THREAD);
```

`monsoon::IOManager` 是项目自研的"线程池 + 协程"调度器（`src/fiber/`），用来跑两个长时间 sleep 的 ticker，省线程。

**5) 启动三大后台循环（1029-1037）**

| Ticker | 跑在 | 作用 | 周期 |
| --- | --- | --- | --- |
| `leaderHearBeatTicker` | **协程** | Leader 周期性广播 AppendEntries（日志或纯心跳） | `HeartBeatTimeout` ≈ 25ms |
| `electionTimeOutTicker` | **协程** | Follower/Candidate 检查心跳超时，超时发起选举 | 随机 [300, 500]ms |
| `applierTicker` | **`std::thread`** | 把 `[lastApplied+1, commitIndex]` 推入 `applyChan` | 不固定 |

**为什么 applier 单独开线程？**（源码注释给出原因）

- 前两个 ticker 时间恒定，sleep 主导，适合协程。
- `applierTicker` 要往 `applyChan` 推消息，下游 KvServer 可能正写 SkipList / 做快照，**执行时间不可控**；用协程会阻塞调度器导致心跳被饿死，故保留独立线程。

> 1039-1046 的注释代码是旧版：三个 ticker 都用 `std::thread`，后来前两个改成了协程。

**6) 调用关系速览**

```
KvServer 构造
   └── Raft::init(peers, me, persister, applyCh)
            ├── 清零易失状态
            ├── readPersist()    ← 磁盘恢复 term/votedFor/logs/快照元信息
            ├── new IOManager
            ├── 协程: leaderHearBeatTicker   ─┐
            ├── 协程: electionTimeOutTicker  ─┼─→ 驱动整个共识算法
            └── 线程: applierTicker          ─┘
```

#### 疑问 / TODO

- [ ] 源码 TODO：崩溃恢复时是否应同时把 `commitIndex = lastSnapshotIncludeIndex`？自己动手改一下，跑测试看是否会引入问题。
- [ ] `m_peers[me]` 在构造时是否被置空？后续读 `RaftRpcUtil` 验证。
- [ ] `FIBER_THREAD_NUM` 默认值多少？在 `src/common/include/config.h` 确认。
- [ ] `applierTicker` 和 `KvServer::ReadRaftApplyCommandLoop` 的协作流程，下次精读 `kvServer.cpp` 时再补一节。

---

### 2026-05-23 · `Raft::electionTimeOutTicker` 选举超时看门狗

- **文件**：`src/raftCore/raft.cpp`
- **行号**：313 ~ 359
- **关键词**：选举超时 / 看门狗 / 随机化 / split vote / 协程 sleep

#### 代码位置

```cpp
void Raft::electionTimeOutTicker();
```

由 `Raft::init()` 作为协程启动（`m_ioManager->scheduler(...)`）。**Raft 节点中 Follower → Candidate 状态转换的唯一触发源**。

#### 核心思想：看门狗模型

把它理解成一条狗，规则："**如果超过 X 时间不喂我，我就叫**"。

| 比喻 | Raft 中对应 |
| --- | --- |
| 狗 | 这个 ticker 循环 |
| "X 时间" | 随机选举超时（300~500ms） |
| 喂狗 | 收到 `AppendEntries` / 给某个 candidate 投票 |
| "上次被喂的时刻" | `m_lastResetElectionTime` |
| 狗叫 | `doElection()` 发起选举 |

**喂狗动作不在本函数里**，而在 `AppendEntries`、`RequestVote` 等 RPC 处理函数里 `m_lastResetElectionTime = now();`。本函数只是定时醒来对账。

#### 代码分块解读

**① Leader 空转避让（315-322）**

```cpp
while (m_status == Leader) {
  usleep(HeartBeatTimeout);  // 25ms
}
```

Leader 自己不需要选举超时（它发心跳，不是收心跳）。但函数是 `while(true)`，必须睡一下，否则：
- 浪费 CPU 空转；
- **更严重**：会饿死同 IOManager 线程上的其他协程（如 `leaderHearBeatTicker`），导致心跳发不出去，引起异常 leader 切换。

**② 算"还要睡多久才到超时点"（323-330）**

```cpp
wakeTime = now();
suitableSleepTime = getRandomizedElectionTimeout()  // [300, 500]ms 随机
                  + m_lastResetElectionTime          // 上次被喂时刻
                  - wakeTime;                        // 当前时刻
```

公式推导（数轴）：

```
●────────────────────●─────────────────●
│                    │                 │
m_lastResetElect..   wakeTime       该叫的时刻
                                    = m_lastResetElect.. + 随机超时

应该睡多久 = 该叫的时刻 - 当前
         = (上次被喂时刻 + 随机超时) - wakeTime
```

变量名 `wakeTime` 命名不佳，**它实际是"睡前时刻"快照**，不是"睡醒时刻"。用作后面对账的基准。

**③ 真的去睡（332-351）**

```cpp
if (suitableSleepTime > 1ms) {
  usleep(...);
}
```

- `> 1ms` 是边界保护：剩余时间已 ≤1ms（甚至负数）就别睡了，直接进入对账 → 立即触发选举。
- `usleep` 在 `monsoon` 协程库里被 hook（`src/fiber/hook.cpp`），**实际是协程让出**：本线程会去跑 `leaderHearBeatTicker` 等其他协程，到点再回来。这是为什么本函数能用协程而不是线程跑的关键。
- 紫色日志对比"计划睡眠 vs 实际睡眠"，方便发现调度异常。

**④ 醒来对账（353-356）—— 整个函数最精华一行**

```cpp
if (m_lastResetElectionTime - wakeTime > 0) {
  continue;  // 睡眠期间被喂过，不超时，重新算
}
```

含义：**"现在的'上次被喂时刻'比我睡前那一刻还要靠后 → 说明在我睡觉这段时间，有人写过 `m_lastResetElectionTime` → 没超时。"**

举例：

```
t=1000  m_lastResetElectionTime = 1000，wakeTime = 1000，决定睡 400ms
t=1200  收到心跳 → m_lastResetElectionTime 改成 1200
t=1400  醒来对账：1200 - 1000 = +200 > 0 → continue 重新算
t=1400  下一轮：1200 + 400 - 1400 = 200ms，再睡 200ms 到 1600

t=1600  若期间没人喂  
        m_lastResetElectionTime(1200) - wakeTime(1400) = -200，不大于 0
        → doElection()
```

**⑤ 真的超时 → 发起选举（357）**

```cpp
doElection();
```

自增 `currentTerm`、转 Candidate、给自己投票、重置选举定时器、并行向所有 peer 发 `RequestVote`。结束后回循环顶部下一轮。

#### 为什么"超时要随机"？

避免 **Split Vote（选票瓜分）**：

- 若所有 Follower 用固定超时，Leader 挂掉后**所有人同时**超时 → 同时 term++ → 同时给自己投票 → 同一 term 每人最多投一次 → 互相不投 → **谁都拿不到多数派** → 不停重选。
- 随机化让某一个节点**先**超时，赶在别人之前把选票收走，**一轮搞定选举**。
- 范围选取经验：下限 ≫ 心跳周期（避免误超时），宽度 ≫ 一次 RPC 时长（让先超时者有时间收完票）。
  - 本项目：心跳 25ms、选举超时 300~500ms（宽 200ms）。

#### 为什么不直接 `sleep(随机超时)` 后判断？

如果睡眠中途被心跳"喂过"，"该叫的时刻"会被推后，再机械睡 400ms 就晚了。`continue` 重新计算的写法保证：**每次喂狗都把闹钟往后推**，行为始终正确。

#### 调用链 / 依赖

- 启动方：`Raft::init` → `m_ioManager->scheduler([]{ electionTimeOutTicker(); })`
- 喂狗方（写 `m_lastResetElectionTime = now()`）：
  - `Raft::AppendEntries`（被动方）：收到合法心跳/日志
  - `Raft::RequestVote`（被动方）：决定投票时
  - `Raft::doElection`：自己发起选举时
  - `Raft::init`：节点初始化时

#### 一句话总结

> **定时醒来看一眼 `m_lastResetElectionTime`，如果在自己睡着期间没被更新过，就发起选举。**

#### 疑问 / TODO

- [ ] `getRandomizedElectionTimeout()` 是否每次调用都重新随机？验证 `src/common/util.cpp` 实现。
- [ ] `usleep` 在 `monsoon` 协程库的 hook 路径，下次读 `src/fiber/hook.cpp` 时跟进。
- [ ] 多个节点起步时随机种子是否独立？（同一秒 fork 出多个进程会不会拿到相同的随机序列）
- [ ] 配套精读 `doElection()` 与对端的 `RequestVote()`，把"先超时者收完票"这条路径在代码里走一遍。

---

### 2026-05-23 · `Raft::doElection` 发起选举 + 五个深入问题

- **文件**：`src/raftCore/raft.cpp`
- **行号**：202 ~ 245
- **关键词**：发起选举 / Election Restriction / 重叠选举 / 喂狗语义 / 活性 vs 安全性

#### 代码位置

```cpp
void Raft::doElection();
```

由 `electionTimeOutTicker` 检测到超时后调用，对应 Raft 论文图 2 中 Candidate 启动选举的 4 步规则：
1. `currentTerm++`
2. vote for self
3. reset election timer
4. send RequestVote RPCs

#### 代码分块解读

**① 整体加锁（203）**

```cpp
std::lock_guard<std::mutex> g(m_mtx);
```

整个函数自始至终持有 `m_mtx`。`m_status / m_currentTerm / m_votedFor` 必须**原子地一起更新并持久化**，否则会产生破坏 Raft 安全性的"半成品状态"。

**② 防御性检查（205-210）**

```cpp
if (m_status != Leader) { ...选举... }
```

防止"已经当上 Leader 的节点因为协程调度延迟，看门狗醒来后又错误地发起选举"。**注意**：这个检查只防"已 Leader"，**不防"还在 Candidate 等结果"**——后者是 Raft 设计上允许的（见后文"五个问题"之 Q3）。

**③ 选举四步走（211-243）**

| 步骤 | 代码 | 说明 |
| --- | --- | --- |
| 转 Candidate | `m_status = Candidate` | |
| term++ | `m_currentTerm += 1` | 逻辑时钟前进 |
| 自投 | `m_votedFor = m_me` | 用掉本任期投票权，**避免给同辈 candidate 投** |
| 持久化 | `persist()` | 论文图 2 要求 `currentTerm/votedFor/logs` 落盘 |
| 共享票数 | `std::shared_ptr<int> votedNum = make_shared<int>(1)` | "亮点"——让所有 RPC 线程共享同一计数器 |
| 重置定时器 | `m_lastResetElectionTime = now()` | 给本轮一个完整的回复窗口（详见 Q1） |
| 并行发送 | 每个 peer 起一个 `std::thread` 跑 `sendRequestVote` | 不能串行（慢）也不能复用本线程（持锁）|

**④ RequestVote 携带四个字段**

```cpp
requestVoteArgs->set_term(m_currentTerm);
requestVoteArgs->set_candidateid(m_me);
requestVoteArgs->set_lastlogindex(lastLogIndex);
requestVoteArgs->set_lastlogterm(lastLogTerm);
```

后两个字段对应 **Election Restriction**（论文 §5.4.1），是 Raft 安全性的基石（详见 Q2）。

#### Q1：为什么要重置 `m_lastResetElectionTime`？（修正版）

之前以为是"防止间隔变成 0"，**这是错的**。`m_lastResetElectionTime` 是绝对时间戳，下一轮看门狗会重算睡眠时长，不会出现 0 间隔。

**真实原因**：**给本轮选举一个完整的"等回复"窗口期**。

不重置会怎样：

```
t=1000  m_lastResetElectionTime = 1000，决定睡 400ms 调 doElection
t=1400  doElection 发出 RequestVote（不重置）
        看门狗 continue 重新算：1000 + 400 - 1400 = 0ms
        → 几乎立刻又触发下一轮 doElection
        → currentTerm 又 +1
        → 上一轮的 RequestVoteReply 回来时被当过期消息丢弃
        → 本来可能赢的那一轮被自己掐死
```

重置后，下一轮老老实实再睡 400ms，给本轮收票时间。

#### Q2：`lastLogIndex` 和 `lastLogTerm` 是什么？是已 commit 的吗？

**不是**。它们是日志数组**最后一条**的下标和当时的 Leader 任期，**已 commit + 未 commit 都算**。

```
索引:    1     2     3     4     5     6     7
       ┌───┬───┬───┬───┬───┬───┬───┐
日志:  │T1 │T1 │T2 │T2 │T3 │T3 │T3 │
       └───┴───┴───┴───┴───┴───┴───┘
                        ↑           ↑
                  commitIndex   lastLogIndex
                     = 4           = 7
```

`getLastLogIndexAndTerm` 拿的就是 `m_logs` 末尾（或 `m_lastSnapshotInclude*` 在快照场景下兜底），**不查 `commitIndex`**。

**为什么不用 `commitIndex` 而用 `lastLogIndex`？**

- 各节点对 commit 的认知是**滞后且不一致**的：日志相同但 commitIndex 可能差很多。
- 用 `lastLogIndex` 比的是**日志内容本身**的新旧，与 commit 滞后无关。
- 而且能证明：**任何能拿到多数派选票的 candidate，必然包含所有已 commit 日志**——因此用 lastLog 反而比用 commitIndex 更安全、更严格。

**Election Restriction 的两条比较规则**：
1. 先比 term：lastLogTerm 大者胜；
2. term 相同再比 index：lastLogIndex 大者胜。

#### Q3：上一轮选举还没结束又开了下一轮，怎么办？

**会发生，且 Raft 设计上允许**。`if (m_status == Leader)` 拦不住"还在 Candidate 等结果"的情况。

```
t=1000  doElection（term=2）发出 RequestVote
t=1400  网络慢，回复还没到 → 看门狗超时 → 又调 doElection
        m_status = Candidate（不是 Leader），通过检查
        currentTerm = 3，重发 RequestVote
t=1500  term=2 的回复终于到达，被 sendRequestVote 当成"过期回复"丢弃
```

**为什么没事**？两个机制兜底：
1. **过期回复丢弃**：`sendRequestVote` 检查 `reply.term < m_currentTerm` 或 `args.term ≠ m_currentTerm` 直接丢；
2. **term 单调递增**：Raft 论文 §5.2 明确写 "When this happens, each candidate will time out and start a new election by incrementing its term"，重叠就是标准恢复路径。

工程层面靠 **"随机超时 ≫ RPC 时长"** 的参数选择让重叠很少发生（300~500ms vs ~50ms）。

#### Q4：喂狗的真实语义是什么？

之前说"喂狗 = 集群健康"，**这只覆盖了一种情况**。三个喂狗时刻并列看：

| 喂狗时刻 | 真实理由 |
| --- | --- |
| 收到合法 AppendEntries | 集群有 Leader 在工作，没必要选举 |
| 给别人投票 | 已选定候选人，**让 ta 先试试**，自己别添乱 |
| 自己发起选举 | 给自己本轮**一个完整的回复窗口** |

统一描述：

> **"喂狗 = 我现在有一个进行中的进展（被服务/等候选人/等自己），请暂时别再开新一轮选举打断它。"**

注意：**作为 candidate 收到 RequestVoteReply 不喂狗**——投票回复只能告知"本轮进展如何"，不构成"应当克制不再开新轮"的理由。

#### Q5：会不会一直选举失败？

**理论上可能（FLP 不可能定理），但工程上概率极低。**

可能让选举一直失败的三种情况：

1. 网络持续不稳定 → candidate 收不到回复；
2. Split Vote 反复瓜分（即使有随机化，极端情况仍可能）；
3. **被分区的少数派持续抬 term** → 网络恢复后干扰多数派 Leader（最阴险）。

Raft 的应对：

- 1)、2) 靠**随机化 + 工程参数**（论文分析：5 节点、150~300ms 超时、~50ms RPC 时，99.9% 的选举一轮完成）；
- 3) 靠 **PreVote 机制**（论文扩展，etcd / TiKV 等生产实现都加了，**本项目未实现**）：candidate 先发"预投票"不抬 term，预投票通过才正式抬 term。

**FLP 定理**：异步网络 + 节点可能崩溃的模型下，没有任何确定性算法能同时保证安全和活性。Raft 的取舍：

| 性质 | 态度 |
| --- | --- |
| Safety（安全） | **绝对保证**——已 commit 数据永不丢/不变 |
| Liveness（活性） | **概率性保证**——网络稳定时能选出 |

> 选不出来时集群进入"无 Leader"状态：客户端写请求得 `ErrWrongLeader` 不停轮询，但**所有数据都在**，网络一恢复立刻继续工作。这是 CAP 里 CP 系统的典型选择：**宁可暂时不可用，也不返回错误数据**。

#### 调用链

```
electionTimeOutTicker（看门狗超时）
        └── doElection
                 ├── currentTerm++ / 自投 / persist / 重置 timer
                 └── 每个 peer 一个 std::thread → sendRequestVote
                         ├── 拿到多数票 → 改 m_status = Leader → 启动 doHeartBeat
                         └── 收到更高 term → 退回 Follower
```

#### 一句话总结

> **`doElection` = 论文图 2 候选人启动选举的代码翻译：term++ → 自投 → 持久化 → 重置定时器 → 并行发 RequestVote。**

#### 代码可改进点

##### ① `votedNum` 不是线程安全的

```cpp
std::shared_ptr<int> votedNum = std::make_shared<int>(1);
```

**问题**：

- 多个 `sendRequestVote` 线程并发地读改 `*votedNum`（`++(*votedNum)` 不是原子的，等价于"读 → 加一 → 写"三步）；
- `std::shared_ptr` **本身的引用计数**是线程安全的，但**它指向的对象**不受这个线程安全保护；
- 极端情况两个线程同时把 `*votedNum` 从 3 加到 4（而不是 5），导致**少数一两票丢失**——可能让本可当选的选举失败。

**修复方案**（任选其一）：

```cpp
// 方案 A：原子计数器（推荐，无锁、最快）
auto votedNum = std::make_shared<std::atomic<int>>(1);
// 累加：votedNum->fetch_add(1, std::memory_order_relaxed);
// 读取：votedNum->load();
```

```cpp
// 方案 B：加锁
// 在 sendRequestVote 里累加 *votedNum 时复用 m_mtx，
// 反正成功当选时也需要持锁切 Leader 状态，统一加锁也合理。
```

**为什么本项目"看起来没事"？** 因为 `sendRequestVote` 里累加 `*votedNum` 时通常**也持有 `m_mtx`**（要同时检查 term、判断是否当选），相当于无意中用大锁兜底了。一旦把那个锁拆细，就会立刻暴露竞态。所以这是**潜在的 bug**，应当显式用 `std::atomic` 表达意图。

> 来源：代码随想录知识星球指出的改进点。

#### 疑问 / TODO

- [ ] 精读 `Raft::sendRequestVote`：过期回复怎么判断、`*votedNum` 怎么累加、当选时如何切 Leader 状态。
- [ ] 精读对端 `Raft::RequestVote`：投票决策（term 比较 + Election Restriction）、为什么投票时要喂狗。
- [ ] 这版代码是否实现了 PreVote？没有的话，自己加上一版作为优化练习。
- [ ] `doElection` 里 `if (m_status == Leader) {}` 这个空 `if` 是历史遗留还是有意为之？查 git log 确认。
- [ ] 验证：本节点如果在发出 RequestVote 后**立刻**收到更高 term 的 AppendEntries，状态机如何收敛？（应当退 Follower，但要看代码确认）
- [ ] **动手优化**：把 `votedNum` 改成 `shared_ptr<atomic<int>>`，跑 caller 验证；再思考是否需要 `memory_order_acq_rel` 之类更强的内存序。

---

### 2026-05-23 · `Raft::sendRequestVote` 投票回复处理

- **文件**：`src/raftCore/raft.cpp`
- **行号**：761 ~ 826
- **关键词**：term 三段比较 / 过期回复丢弃 / 多数派当选 / nextIndex 与 matchIndex / "网络 I/O 不持锁"

#### 代码位置

```cpp
bool Raft::sendRequestVote(int server,
                           std::shared_ptr<RequestVoteArgs> args,
                           std::shared_ptr<RequestVoteReply> reply,
                           std::shared_ptr<int> votedNum);
```

由 `doElection` 给每个 peer 起一个独立 `std::thread` 调用。**职责 = 发一次 RPC + 处理回复**——后者才是真正的核心。

#### 代码分块解读

**① 网络 RPC 阶段（766-774）**

```cpp
bool ok = m_peers[server]->RequestVote(args.get(), reply.get());
if (!ok) return ok;
```

- `m_peers[server]` 是 `RaftRpcUtil`，内部走 `MprpcChannel`（自研 RPC）发到对端的 `Raft::RequestVote`。
- **关键注释**："这个 ok 是网络是否正常通信的 ok，**而不是 requestVote rpc 是否投票的 rpc**"。
  - 是否同意投票 → `reply->votegranted()`
  - 对方当前 term → `reply->term()`
- **为什么不死循环重试？** 因为 Raft 容错不依赖"必须送达"，只依赖"凑齐多数派"。死等会卡线程、且等通时回复已过期。看门狗超时后下次再来即可。

**② RPC 之后才加锁（783）**

```cpp
std::lock_guard<std::mutex> lg(m_mtx);
```

Raft 实现通用模式：**「短锁 + 网络 I/O 在锁外」**。

- 网络 I/O 阻塞几十~几百毫秒，持锁等它会卡住整个节点；
- 但后面要改全局状态（term/status/votedFor/nextIndex/matchIndex），必须加锁。

**③ term 三段比较（784-793）—— Raft 通用规则**

| 情况 | 处理 |
| --- | --- |
| `reply.term > m_currentTerm` | **三变**：身份 → Follower、term → reply.term、`m_votedFor = -1`，`persist()` 落盘 |
| `reply.term < m_currentTerm` | 这是上一轮的"**过期回复**"，直接丢弃 |
| `reply.term == m_currentTerm` | 正经回复，继续处理（`myAssert` 兜底） |

第一种对应论文图 2 末尾的全局规则："If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower."

第二种就是上一节"重叠选举"讨论里那个"过期回复丢弃"机制的真实落点。

**④ 是否同意投票（796-798）**

```cpp
if (!reply->votegranted()) return true;
```

term 相等但对方不投，可能是"已投给别人"或"我日志不够新（Election Restriction）"。

**⑤ 累加票数（800）—— 潜在线程安全点**

```cpp
*votedNum = *votedNum + 1;
```

读-加-写三步，多线程并发理论上可能丢更新。**目前能正确运行是因为外层 `m_mtx` 在串行化**——这是上一节已记录的改进点。

**⑥ 多数派判定 + 切 Leader（801-824）**

```cpp
if (*votedNum >= m_peers.size() / 2 + 1) {
  *votedNum = 0;                     // 防重入：后续回复别再进这个分支
  if (m_status == Leader) myAssert(false, ...);  // 安全断言
  m_status = Leader;
  int lastLogIndex = getLastLogIndex();
  for (int i = 0; i < m_nextIndex.size(); i++) {
    m_nextIndex[i]  = lastLogIndex + 1;  // 乐观：先假设对方一样长
    m_matchIndex[i] = 0;                 // 保守：未确认任何匹配
  }
  std::thread t(&Raft::doHeartBeat, this);  // 立刻发心跳，开新线程避免自锁
  t.detach();
  persist();
}
```

几个关键点：

- **`*votedNum = 0` 是廉价的"只执行一次"**：阈值满足后第一个线程进 if 分支切 Leader，把计数器清零，**后续陆续到达的回复**累加只会拿到 1/2/3 等小值，再也凑不满阈值，避免重复执行切换逻辑。更优雅的写法是 `std::call_once` 或检查 `m_status == Candidate`，但这种简易写法配合下面的 `assert` 也能工作。
- **`nextIndex` 乐观、`matchIndex` 保守**：

  | 数组 | 初值 | 用途 | 初始化策略 |
  | --- | --- | --- | --- |
  | `nextIndex[i]` | `lastLogIndex + 1` | AppendEntries 起点 | **乐观**：先假设对方一样长，不匹配再回退 |
  | `matchIndex[i]` | `0` | 算 `commitIndex` 用 | **保守**：没确认前不算复制成功 |

  这对应"试错可以快、提交必须稳"的设计哲学。

- **立刻发心跳的两个目的**：
  1. 防止其他 Follower 因没收到本节点心跳而继续超时发选举，干扰新 Leader 地位；
  2. 让集群快速感知新 Leader。
- **为什么开新线程跑 `doHeartBeat`？** 本函数仍持 `m_mtx`，`doHeartBeat` 也要拿这把锁——同步调会死锁。开线程让它在锁释放后再跑。
- **末尾的 `persist()` 有点冗余**：term/votedFor/logs 都没被修改（doElection 时已落盘过），这里再持久化一次只是"保险"。

#### 防御矩阵

| 担心的事 | 防御代码 |
| --- | --- |
| 网络断了死等 | `if (!ok) return;` |
| 收到过期 term 回复 | `else if (reply.term < currentTerm) return;` |
| 收到更高 term 回复 | "三变 + persist + 退 Follower" |
| 同 term 多次切 Leader | `*votedNum = 0` + `myAssert` |
| 同步调 doHeartBeat 自锁 | 开新线程 detach |
| `*votedNum` 多线程累加 | 由外层 `m_mtx` 兜底（潜在改进） |

#### 调用链

```
doElection
  └── 每 peer 一个 std::thread → sendRequestVote
                                       │
                                       ├── 网络 RPC（无锁阶段）
                                       ├── 加锁
                                       ├── term 三段比较
                                       │     ├── 高 → 退 Follower
                                       │     ├── 低 → 丢弃
                                       │     └── 等 → 继续
                                       ├── 票数累加
                                       └── 多数派 → 转 Leader → 起 doHeartBeat 新线程
                                                                       │
                                                                       └── 周期发 AppendEntries
```

#### 一句话总结

> **`sendRequestVote` = 发一次 RequestVote → 按 Raft 通用规则比 term → 累加票 → 攒够多数派就当 Leader 并立刻发心跳。**

#### 疑问 / TODO

- [ ] `doHeartBeat` 怎么实现的？心跳如何携带日志、如何处理 `nextIndex` 回退？（下次精读）
- [ ] `*votedNum = 0` 这种"清零防重入"在更复杂场景下会不会有歧义？比如和"票太少没当选但回复继续到达"的状态怎么共存？
- [ ] 824 行 `persist()` 能否安全删除？写个最小验证看看是否真的冗余。
- [ ] 验证"网络 I/O 不持锁"模式：故意把 `RequestVote` 调用放进锁内，看 caller 延迟会涨成什么样。
- [ ] `myAssert(false, "同一个 term 当两次领导")` 在哪些异常路径下可能被触发？想象一种能让它命中的场景作为反例。

---

### 2026-05-24 · `Raft::RequestVote` 投票决策（被动方）+ 安全性证明 + 概念辨析

- **文件**：`src/raftCore/raft.cpp`
- **行号**：611 ~ 683（含 `UpToDate` 685 ~ 692）
- **关键词**：投票决策 / Election Restriction / 鸽巢原理 / DEFER persist / 多数派复制 / 复制状态机

#### 代码位置

```cpp
void Raft::RequestVote(const RequestVoteArgs* args, RequestVoteReply* reply);
```

被动方处理拉票请求，对应论文图 2 **RequestVote RPC** 的接收方两条规则：

1. Reply false if `term < currentTerm`
2. If `votedFor == null || votedFor == candidateId` **and** candidate's log is at least as up-to-date → grant vote

#### 代码分块解读

**① 加锁 + DEFER 持久化（612-618）**

```cpp
std::lock_guard<std::mutex> lg(m_mtx);
DEFER { persist(); };
```

利用 RAII 的**逆序析构**：

```
栈构造顺序：lg → DEFER 对象
栈析构顺序：DEFER 析构（调 persist）→ lg 析构（释放锁）
```

> **持久化先发生，然后才释放锁**——注释里明确写"应该先持久化，再撤销 lock"。

为什么必须这样？反例：

```
1. 决定投票给 X，m_votedFor = X
2. 释放锁
3. 还没 persist 就崩溃
4. 重启后 m_votedFor 又是 -1
5. 同 term 的 Y 来要票，又投了
6. → 同一 term 投了两次票 → 破坏 Raft 安全性
```

**额外好处**：函数有 5 处 return，DEFER 自动覆盖所有路径，避免漏写 persist。

**② term 三段比较（621-640）**

| 情况 | 行为 | 与 `sendRequestVote` 的关键差异 |
| --- | --- | --- |
| `args.term < currentTerm` | 拒绝（`Expire`），不喂狗，把 `m_currentTerm` 写进 reply | — |
| `args.term > currentTerm` | **三变 + 继续往下**（不直接 return） | 主动方是"三变后 return"，被动方还要继续判断要不要授票 |
| `args.term == currentTerm` | `myAssert` 兜底，继续往下 | — |

**③ 日志新旧检查（643-662）—— Election Restriction**

```cpp
if (!UpToDate(args->lastlogindex(), args->lastlogterm())) {
  reply->set_votegranted(false);
  return;
}
```

不通过 → 拒绝（`Voted` 状态码，**注意：这里语义复用了"已投票"，是项目里的小瑕疵**）。

**关键：被拒绝时不喂狗**——"我没承诺任何东西，没有进行中的进展需要保护"，所以不重置定时器。

**④ 是否已投票检查（663-682）**

```cpp
if (m_votedFor != -1 && m_votedFor != args->candidateid()) {
  reply->set_votegranted(false);
  return;
} else {
  m_votedFor = args->candidateid();
  m_lastResetElectionTime = now();    // ← 仅授票时喂狗
  reply->set_votegranted(true);
  return;
}
```

授票时做三件事：记 votedFor、**喂狗**、回 true。

> 注释里那句"**必须要在投出票的时候才重置定时器**" 验证了上一节关于"喂狗的真实语义 = 给进行中的进展留时间"。

#### 函数决策树

```
收到 RequestVote
   │
   ├── args.term < currentTerm                 → 拒绝(Expire)，不喂狗
   │
   ├── args.term > currentTerm                 → 降级 + term 拉齐 + votedFor=-1，继续
   │
   ├── (args.term == currentTerm)
   │     │
   │     ├── candidate 日志不够新               → 拒绝(Voted)，不喂狗
   │     │
   │     ├── 本 term 已投给"别人"               → 拒绝(Voted)，不喂狗
   │     │
   │     └── 没投过 / 投给了同一个 candidate    → 投票 + 喂狗 → 同意(Normal)
   │
   └── DEFER: persist() 再释放锁
```

#### 主动方 vs 被动方差异速览

| 维度 | sendRequestVote（主动） | RequestVote（被动） |
| --- | --- | --- |
| 触发 | 自己定时器超时 | 收到 RPC |
| term 高时 | 三变后**直接 return** | 三变后**继续往下**（还可能授票） |
| 持久化 | 显式 `persist()` | `DEFER` 自动 |
| 喂狗 | 不喂 | 仅授票时喂 |

---

#### Q1：为什么 `sendRequestVote` 收到回复后还要再比 term？不是被动方比过了吗？

**不是重复比较，是两个方向各自防御**：

| 函数 | 比谁 | 防什么 |
| --- | --- | --- |
| `RequestVote`（被动） | `args.term` vs `m_currentTerm` | 对方带着的 term 落后于我 |
| `sendRequestVote`（主动） | `reply.term` vs `m_currentTerm` | 等回复期间**我自己变过时了** |

具体场景：

```
t=1000  candidate 用 term=5 发 RequestVote
t=1100  candidate 期间收到别人 term=10 的 AppendEntries
        → 已退 Follower，currentTerm = 10
t=1200  原 RequestVote 的回复才到，reply.term = 6
        → 主动方比：reply.term(6) < my(10) → 这是过期回复，丢弃
```

如果不比，会拿过期回复继续累加 votedNum，可能误以为当选——破坏安全性。

**论文图 2 末尾全局规则**："RPC **request or response** contains term T > currentTerm: convert to follower"。**两个方向都要比**，缺一不可。

#### Q2：`reply->set_votestate(...)` 这些字段真的没被读？

**确认：没被读**。

```cpp
// sendRequestVote 里只用了 reply->term() 和 reply->votegranted()
// votestate 从头到尾没被取出
```

可能的原因：
- 历史遗留 / 调试用（开发期想区分多种拒绝原因）；
- 预留字段（以后加 PreVote 或精细诊断时用）；
- MIT 6.824 lab 痕迹。

**结论**：这是项目里的小冗余，不影响正确性，但 `Voted` 状态被复用为"日志旧"和"已投他人"两种含义，**语义不清**。可以记到 TODO 里清理或合理利用。

#### Q3：`UpToDate` 是什么？

实现就 4 行（`raft.cpp:685-692`）：

```cpp
bool Raft::UpToDate(int index, int term) {
  int lastIndex = -1;
  int lastTerm = -1;
  getLastLogIndexAndTerm(&lastIndex, &lastTerm);
  return term > lastTerm || (term == lastTerm && index >= lastIndex);
}
```

参数是 candidate 的 `lastLogIndex/lastLogTerm`，跟自己的比较，规则就是 Raft 论文 §5.4.1 的 Election Restriction：

```
candidate 至少和我一样新 ⇔
   candidate.lastTerm > my.lastTerm
   或 candidate.lastTerm == my.lastTerm 且 candidate.lastIndex >= my.lastIndex
```

#### Q4：怎么保证"新 Leader 包含所有已 commit 日志"？（鸽巢原理）

> **被多数派复制 + UpToDate 投票限制 + 鸽巢原理 = Raft 安全性。**

**反证步骤**：

1. 设某条日志 L 已 commit → L 至少存在于一个多数派 A 中；
2. 任何当选的新 Leader 需要拿到多数派 B 的票；
3. **鸽巢原理**：A 和 B 都是多数派，必有交集 P；
4. P ∈ A → P 拥有 L；
5. P ∈ B → P 给 candidate 投了票；
6. P 投票时通过了 `UpToDate` 检查 → `candidate.lastLog ≥ P.lastLog`；
7. 而 P 包含 L → candidate 也必然包含 L。

**结论**：**新 Leader 必然包含 L**，不可能用旧日志覆盖已 commit 的内容。

```
集合 A（拥有 L 的节点）：[N1, N2, N3]   ← 多数派
集合 B（给 candidate 投票的）：[N3, N4, N5] ← 多数派

         ┌────────┐
         │ N3 ∈ A∩B│ ← 鸽巢交集
         └────────┘
N3 投票通过 UpToDate → candidate 至少和 N3 一样新 → 包含 L
```

UpToDate 是这个证明的"门卫"——没有它，鸽巢节点 P 可能投给不包含 L 的 candidate，已 commit 数据就会丢。

> 论文 §5.4.2 还有 Figure 8 的细节：commit 必须是当前 term 的日志。这部分等读 `updateCommitIndex` 再展开。

#### Q5：candidate 的"重发 RequestVote" 在哪实现的？

**重要更正**：上一轮我说"同 term 重发能命中 `votedFor == candidateId` 分支"，但**核查代码后发现本项目没有这个重发逻辑**。

```cpp
// sendRequestVote
if (!ok) {
  return ok;        // 网络失败直接 return，不重试
}
```

本项目的失败兜底完全靠 **"看门狗超时 → 抬 term → 重新选"**，term 已经变了，走的是 `m_votedFor == -1` 分支（高 term 重置 votedFor）而不是 `m_votedFor == candidateId` 分支。

那源码里 663-664 行的注释 "因为网络质量不好导致请求丢失重发就有可能" 描述的是什么？

- 是 **论文 / 通用 Raft 实现** 的角度（保证 RequestVote 幂等性）；
- 在 **MIT 6.824 lab** 里测试框架可能模拟同 term 重发；
- 在本项目里这个分支几乎走不到，**作为防御性编程保留**。

---

#### 概念辨析（重要）：日志 / 写入本地 / 提交 / 应用到状态机

这四个词是 Raft 最容易混淆的地方。**本质是一条流水线的四个阶段**，对应一次 Put 请求的处理：

| 阶段 | 名字 | 含义 | 代码体现 | 客户端能感知 | 数据安全 |
| --- | --- | --- | --- | --- | --- |
| ① | **日志（log entry）** | "要执行什么命令"的记录，**不是结果** | `LogEntry { index, term, command }` | ❌ | ❌ |
| ② | **写入本地** | "我自己记下来了" | `m_logs.push_back()` + `persist()` | ❌ | ❌ 单节点写还可能丢 |
| ③ | **提交（commit）** | "已被多数派复制，永远不会丢" | 推进 `m_commitIndex` | ❌（即将） | ✅ |
| ④ | **应用（apply）** | "真正执行命令、改业务数据" | `applyChan` → KvServer 改 SkipList | ✅（收回复） | ✅ |

##### 一次 Put 的完整时间线

```
Client: Put("balance_xiaoming", 100)
   │
   ▼
[① 生成日志]  Leader A 把请求包成 LogEntry{index=7, term=5, command=Put...}
[② 写入本地]  A.m_logs.push_back + persist
[② 复制到 B/C]  AppendEntries → B/C 各自写入本地
[③ 提交]      A 看到多数派(A+B+C=3) → m_commitIndex = 7
[④ 应用]      A 的 applierTicker → applyChan → KvServer.skipList.insert(...)
              ───→ 客户端收到 OK
[③ 同步 commit] A 通过下次心跳告诉 B/C/D/E：commitIndex=7
[④ Follower 应用] B/C/D/E 各自 apply 到自己的 SkipList
```

##### 三个核心指针的关系

每个节点都同时维护：

```
索引:    1    2    3    4    5    6    7    8
       ┌──┬──┬──┬──┬──┬──┬──┬──┐
日志:  │T1│T1│T2│T2│T3│T3│T5│T5│
       └──┴──┴──┴──┴──┴──┴──┴──┘
                  ↑       ↑       ↑
           lastApplied commitIndex lastLogIndex
              = 4         = 6        = 8
```

不变量：**`lastApplied ≤ commitIndex ≤ lastLogIndex`**。

- `(commitIndex, lastLogIndex]`：已写入本地但**未 commit**——可能成功（再凑齐多数派就 commit）也可能被新 Leader 覆盖。
- `(lastApplied, commitIndex]`：已 commit 但**未 apply**——必然会被 apply，只是稍微滞后。

##### 为什么要拆这四步？

Raft 是**复制状态机模型**：
> 相同初始状态 + 相同确定性命令 + 相同顺序执行 = 相同最终状态

要做到这点必须：
1. 先记日志再执行（让所有节点能"重放"出相同状态）；
2. 等多数派才执行（否则单节点改了别人不知道）；
3. 分离 commit 和 apply（commit 是算法层判断"何时安全"，apply 是业务层动作"真正改数据"，分层让代码清晰）。

##### "多数派复制"的意义

| 选择 | 后果 |
| --- | --- |
| 只要 Leader 写就算 commit | Leader 一挂数据丢 → 客户端被骗 |
| 全部节点写完才算 commit | 任何一个节点宕机集群就停摆 → 没了高可用 |
| **多数派写完就算 commit** ✅ | 容忍 ⌊(n-1)/2⌋ 个故障，且通过鸽巢原理保证未来 Leader 一定包含 |

容错公式：**`n` 个节点能容忍 `⌊(n-1)/2⌋` 个故障**

| n | 多数派阈值 | 容错数 |
| --- | --- | --- |
| 3 | 2 | 1 |
| 5 | 3 | 2 |
| 7 | 4 | 3 |

→ 这就是 Raft 集群推荐**奇数节点**的原因（5 比 4 多容 1 故障，成本只多 1 份）。

#### 调用链

```
candidate.sendRequestVote ──RPC──► follower.RequestVote
                                        │
                                        ├── 加锁 + 注册 DEFER persist
                                        ├── term 三段比较
                                        │     ├── 低 → 拒绝(Expire)
                                        │     ├── 高 → 降级 + 继续
                                        │     └── 等 → 继续
                                        ├── UpToDate 日志新旧检查
                                        ├── votedFor 检查
                                        └── 同意 → votedFor = candId + 喂狗 + 回 true
                                        └── DEFER 触发：persist → 释放锁
```

#### 一句话总结

> **`RequestVote` = 比 term → 看日志够不够新（UpToDate）→ 看本 term 投过没 → 同意就喂狗+回 true，拒绝就回 false。**
>
> **核心安全保证：DEFER 确保"持久化先于解锁"，配合鸽巢原理证明，让"已 commit 的日志永远不会被新 Leader 覆盖"。**

#### 疑问 / TODO

- [ ] 清理或合理利用 `votestate` 字段：要么删掉，要么细化拒绝原因（`LogStale` / `AlreadyVoted` / `Expire`），让接收方诊断更清晰。
- [ ] 实现 candidate 主动重发逻辑（带超时和指数退避），观察对选举耗时的影响。
- [ ] 跑一个崩溃测试：在投票后、persist 之前杀掉进程，重启后看是否会出现"同 term 投两次"。难点是怎么注入这个时机——可能要改 `persist()` 加注入点。
- [ ] 论文 §5.4.2 Figure 8 反例的具体场景：自己手画一遍，理解为什么"必须 commit 当前 term 的日志"，下次读 `updateCommitIndex` 再印证。
- [ ] 写一个最小复制状态机 demo（5 个 SkipList + 一个广播器），不带选举只做日志同步，体会"相同初始状态 + 相同顺序命令 = 相同最终状态"。

---

### 2026-05-28 · `Raft::leaderHearBeatTicker` 心跳闹钟

- **文件**：`src/raftCore/raft.cpp`
- **行号**：480 ~ 526
- **关键词**：心跳定时器 / 自驱动 / 与选举 ticker 对偶 / 心跳 ≡ 空 AppendEntries

#### 代码位置

```cpp
void Raft::leaderHearBeatTicker();
```

由 `Raft::init` 作为协程启动（与 `electionTimeOutTicker` 一起调度在同一个 `IOManager` 上）。Leader 用它**定期触发 `doHeartBeat()`**，向所有 Follower 发 `AppendEntries`（携带日志或纯心跳）。

#### 与 `electionTimeOutTicker` 的对偶关系（最重要）

二者结构几乎一模一样（"睡到点 + 醒来对账"），只是身份和触发动作相反：

| 维度 | `electionTimeOutTicker` | `leaderHearBeatTicker` |
| --- | --- | --- |
| 谁用 | Follower / Candidate | **Leader** |
| "在岗"判定 | `m_status != Leader` | `m_status == Leader` |
| 时间戳变量 | `m_lastResetElectionTime` | `m_lastResetHearBeatTime` |
| 周期 | 随机 [300, 500]ms | 固定 25ms（`HeartBeatTimeout`） |
| 超时后触发 | `doElection()`（发 RequestVote） | `doHeartBeat()`（发 AppendEntries） |
| 喂狗者 | **被别人喂**（收 AE / 投票时） | **自己喂自己**（doHeartBeat 内部更新） |
| 心智模型 | 看门狗（被动） | 闹钟（主动） |

> **本质区别**：Follower 等通知，Leader 自驱动。

#### 代码分块要点

**① 非 Leader 时空转避让（483-486）**

```cpp
while (m_status != Leader) {
  usleep(1000 * HeartBeatTimeout);
}
```

非 Leader 跳过本 ticker，避免空转拿锁影响性能。

**② 算睡眠时长（489-495）**

```cpp
suitableSleepTime = std::chrono::milliseconds(HeartBeatTimeout)
                  + m_lastResetHearBeatTime
                  - wakeTime;
```

公式同选举 ticker：`下次该发心跳的时刻 - 当前时刻`。

**③ 真的去睡（497-517）**

- `> 1ms` 边界保护（已经迟到就别睡，立刻发）；
- `usleep` 在 `monsoon` 协程库里被 hook，**实际是协程让出**而非阻塞线程；
- `static atomic<int32_t> atomicCount` 仅用于日志计数，与逻辑无关。

**④ 醒来对账（519-522）**

```cpp
if (m_lastResetHearBeatTime - wakeTime > 0) {
  continue;
}
```

如果"上次心跳时刻"被推到了我睡前那一刻之后 → **别人已经替我发过心跳了**（最常见的就是刚当选时 `sendRequestVote` 立刻起线程调一次 `doHeartBeat`），这次别重复发，重新算时间再睡。

**⑤ 触发心跳（524）**

```cpp
doHeartBeat();
```

`doHeartBeat` 内部会：
1. 给每个 peer 决定发普通 `AppendEntries` 还是 `InstallSnapshot`（看 `nextIndex` vs `lastSnapshotIncludeIndex`）；
2. 并行起线程发 RPC；
3. **顺手 `m_lastResetHearBeatTime = now()`** ← Leader 自己喂自己；
4. 处理回复（推进 `nextIndex` / `matchIndex` / `commitIndex`）。

#### 关键认知：心跳 = 空的 AppendEntries

Raft 协议**没有专门的"心跳消息"**——所谓心跳就是 `entries` 字段为空的 `AppendEntries`。

| 形态 | 用途 |
| --- | --- |
| `AppendEntries(entries=[])` | 纯心跳：维持权威、阻止 Follower 选举超时 |
| `AppendEntries(entries=[...])` | 日志复制：携带新日志 |

**两者是同一条 RPC 走同一段代码**。所以"心跳定时器"和"日志复制"用的是同一套机制——只要 Leader 还在，每 25ms 都会借这个机会顺便把新日志推过去。

#### 为什么心跳周期 ≪ 选举超时？

```
心跳周期 25ms，选举超时 300~500ms（比例 ≈ 1:12 ~ 1:20）

Leader 心跳节奏：●─●─●─●─●─●─...（每 25ms）
Follower 收一次心跳 → 喂选举狗一次
                                       ↑
若 Leader 挂了：●─●─×──── 350ms 没心跳 ────→ 触发选举
```

经验值：**心跳周期 ≈ 选举超时下限的 1/10**。

- 心跳太长 → Leader 没挂但 Follower 误以为它挂 → 不必要的选举；
- 心跳太短 → 浪费带宽，但不影响正确性。

#### 单位 bug 嫌疑（核查后撤回）

读代码时怀疑过 484 行 `usleep(1000 * HeartBeatTimeout)` 单位有问题，**核查 `src/common/include/config.h` 后确认无 bug**：

```cpp
const int HeartBeatTimeout = 25 * debugMul;  // ms
```

- `HeartBeatTimeout = 25`，单位**毫秒**（注释明确）；
- `usleep` 单位是**微秒**；
- `1000 * 25 = 25000us = 25ms` ✅ 正确；
- 494 行 `std::chrono::milliseconds(HeartBeatTimeout)` = 25ms ✅ 正确。

> **经验教训**：怀疑代码有 bug 之前，先把"魔法常量"的真实单位/含义查清楚。这次怀疑站不住脚，但养成"主动核查"的习惯比"避免误判"更有价值。

#### 调用链

```
Raft::init
   └── m_ioManager->scheduler([] { leaderHearBeatTicker(); })
                                       │
                                       ├── 非 Leader 空转
                                       ├── 算睡眠 + sleep
                                       ├── 醒来对账（被别处发过就跳过）
                                       └── doHeartBeat()
                                              ├── 决定 AE vs InstallSnapshot
                                              ├── 并行发 RPC
                                              ├── 更新 m_lastResetHearBeatTime
                                              └── 处理回复（推进 nextIndex/matchIndex/commitIndex）
```

#### 一句话总结

> **`leaderHearBeatTicker` = Leader 的心跳闹钟。结构和选举看门狗一模一样（睡到点 + 醒来对账），只是身份相反、由自己喂自己；触发的 `doHeartBeat` 既维持权威又顺便复制日志，二合一。**

#### 疑问 / TODO

- [ ] `doHeartBeat` 完整实现：`nextIndex` / `matchIndex` 怎么用、不匹配时怎么回退、`InstallSnapshot` 何时触发？（下次精读）
- [ ] 481 行注释提到"目前是睡眠，后面再优化优化"——能否用条件变量代替轮询？写个对比 demo。
- [ ] `atomicCount` 是 `static`，多 Raft 实例（如果改成线程内多节点）会被共享，验证一下当前 fork 多进程场景下是否真的隔离。
- [ ] 把心跳周期从 25ms 改到 50ms / 100ms / 200ms，观察 caller 写入延迟与 leader 切换频率的变化。
- [ ] 验证："刚当选时立刻起线程调 `doHeartBeat`" 和"本 ticker 自然超时调 `doHeartBeat`"两条路径竞争时，是否会出现重复发心跳——理论上 519-522 的对账应该能拦住，写个日志验证一下。

---

### 2026-05-29 · `Raft::doHeartBeat` 广播 AE / 日志复制 + 七个深入问题

- **文件**：`src/raftCore/raft.cpp`
- **行号**：247 ~ 311
- **关键词**：AppendEntries / 心跳与日志复制合一 / Log Matching Property / 快照压缩 / 幂等性 / shared_ptr 轮次令牌

#### 代码位置

```cpp
void Raft::doHeartBeat();
```

由 `leaderHearBeatTicker` 每 25ms 触发一次，也会在"刚当选 Leader"时被 `sendRequestVote` 立即调用一次。**职责**：给所有 Follower 广播 AE（带日志或纯心跳），并喂自己一次心跳狗。

#### 函数全貌（两件事）

```
1. 给每个 Follower 准备一份"专属的" AppendEntries 请求 → 起线程并行发
2. 重置心跳定时器（喂自己一次：m_lastResetHearBeatTime = now()）
```

> **关键认知**：Leader 给每个 Follower 发的 AE **内容是不同的**——因为每个 Follower 的 `nextIndex[i]` 不一样。这是和绝大多数广播协议最大的区别——**广播但内容个性化**。

#### 代码分块解读

**① 加锁 + 状态检查（248-250）**

```cpp
std::lock_guard<std::mutex> g(m_mtx);
if (m_status == Leader) { ... }
```

- 加锁保护下面要读的 `m_nextIndex / m_logs / m_currentTerm / m_commitIndex / m_lastSnapshotIncludeIndex` 等共享状态；
- 双重确认 `m_status == Leader`：ticker 进来时检查过一次，但拿锁过程可能被改（收到更高 term 退 Follower），这里再兜底。**身份不对时不更新 `m_lastResetHearBeatTime`**。

**② 共享计数器 `appendNums`（252）**

```cpp
auto appendNums = std::make_shared<int>(1);  // 含 Leader 自己已"复制"
```

和 `doElection` 的 `votedNum` 完全对称的设计。每次心跳新建一个，所有同轮 `sendAppendEntries` 线程共享，达多数派触发推进 commitIndex。

**③ 决定发 AE 还是 InstallSnapshot（264-271）**

```cpp
if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex) {
  std::thread t(&Raft::leaderSendSnapShot, this, i);
  t.detach();
  continue;
}
```

如果 Follower 的 `nextIndex` 落到了 Leader 已经压缩掉的范围内，AE 没法发（因为 `m_logs` 里已经没那段日志了）→ 改发整个快照。

**④ 构造 AE 请求（272-294）**

| 字段 | 含义 |
| --- | --- |
| `term` | 让 Follower 比 term，触发降级或拒绝 |
| `leaderId` | Follower 知道往哪转发客户端请求 |
| `prevLogIndex / prevLogTerm` | **核心字段**：日志匹配性质的锚点 |
| `entries` | 从 `nextIndex[i]` 开始的所有未复制日志 |
| `leaderCommit` | Leader 的 commitIndex，让 Follower 推进 apply |

**填充 entries 分两个分支**：

- 普通：`for j = getSlicesIndexFromLogIndex(prevLogIndex)+1 ... m_logs.size()`，把全局 index 转换成数组下标；
- 边界：`prevLogIndex == m_lastSnapshotIncludeIndex` 时直接遍历整个 `m_logs`（避免对已压缩 index 调 `getSlicesIndexFromLogIndex`）。

**⑤ 不变量断言（297-299）**

```cpp
myAssert(prevlogindex() + entries_size() == lastLogIndex, ...);
```

物理含义：**"我把 prev 之后到末尾的所有日志都打包发给你了，没漏没多"**。是构造逻辑的一致性兜底。

**⑥ 起线程并行发（301-307）**

```cpp
appendEntriesReply->set_appstate(Disconnected);  // 悲观初值
std::thread t(&Raft::sendAppendEntries, this, ..., appendNums);
t.detach();
```

- `appstate=Disconnected` 是防御性悲观初值——RPC 没成功的话上层读到这个直接跳过；
- 每 peer 一线程：避免串行慢、也避免在持锁时同步调 `sendAppendEntries` 自锁。

**⑦ 喂自己一次心跳狗（309）**

```cpp
m_lastResetHearBeatTime = now();   // 注意：不等回复
```

记录"我（Leader）刚发了一轮 AE 出去"，让 ticker 下轮按 25ms 节奏继续。**不是**记录"广播成功"——心跳节奏和回复处理是解耦的。

---

#### Q1：为什么 `doHeartBeat` 要加锁？不加会怎样？

锁保护的是**共享状态的一致快照**。函数体读了 7~8 个变量（`m_status / m_currentTerm / m_commitIndex / m_nextIndex / m_lastSnapshotIncludeIndex / m_logs / m_lastResetHearBeatTime`），它们在被多个线程并发读写：

- `electionTimeOutTicker` / `leaderHearBeatTicker` 协程；
- N 个 `sendAppendEntries` / `sendRequestVote` 线程；
- `applierTicker` 线程；
- 上层 `KvServer::Start()`（写新日志到 `m_logs`）；
- RPC 框架的回调线程（`AppendEntries` / `RequestVote` / `InstallSnapshot` 被动方）。

**两个典型灾难场景**：

##### 场景 A：term 撕裂

```
线程 1（doHeartBeat）：读 m_currentTerm = 5，设到 args
线程 2（sendRequestVote 收到更高 term）：把 m_currentTerm 改成 8，m_status 改成 Follower
线程 1：继续以"已经不是 Leader"的状态发 AE，term=5 被 Follower 拒绝
```

##### 场景 B：日志数组撕裂（UB）

```
线程 1（doHeartBeat）：正在 for j = ... j < m_logs.size() 遍历
线程 2（KvServer::Start）：m_logs.push_back(newEntry) → vector 重分配 → 之前的指针/迭代器失效
线程 1：*sendEntryPtr = m_logs[j] → 访问已释放内存 → UB / 崩溃
```

> `std::vector` 不是线程安全的，并发 `push_back` + 遍历是经典 UB。

**结论**：加锁的代价（持锁期间起线程会短暂等本函数返回）远小于不加锁的代价（数据撕裂、UB、协议错乱）。

---

#### Q2：什么是快照压缩？为什么要做？

##### 不做会怎样

`m_logs` 只增不减。1 小时几百 MB，1 天几十 GB → **磁盘爆炸 + 重启慢 + 新节点同步慢**。

##### 关键观察

日志只是"待执行命令"的记录。一旦命令已 apply 到状态机：

```
m_logs[1] = Put("x", 10)   ← 已 apply
m_logs[2] = Put("x", 20)   ← 已 apply（覆盖了 1）
m_logs[3] = Put("y", 5)    ← 已 apply
```

SkipList 里就是 `x=20, y=5`。这些日志已经"无用"——直接拷贝 SkipList 即可。

##### 快照（Snapshot）= 把"历史"压缩成"现状"

```
快照内容 = {
  lastIncludeIndex: 3,
  lastIncludeTerm:  2,
  state_machine:    序列化的 SkipList { x=20, y=5 }
}
```

存盘后：

```
m_logs 被截断，从 4 开始：[Put("z",1), Put("z",2)]
```

##### 快照后的索引体系

```
全局逻辑索引：     1    2    3    4    5
                ┌──┬──┬──┬──┬──┐
状态：           │ ─── 已快照 ─── │ z=1│ z=2│
                └──┴──┴──┴──┴──┘
                              ↑
                 lastSnapshotIncludeIndex = 3

实际 m_logs 数组：[ z=1, z=2 ]   ← 只存 4、5
                  下标 0  1
```

→ 这就是为什么需要 `getSlicesIndexFromLogIndex(globalIndex)` 把全局 index 翻译成数组下标。

##### 快照在协议中的两个用途

- **节点重启**：直接还原 SkipList，跳过几亿条日志重放；
- **同步落后 Follower**：当 `nextIndex[X] <= lastSnapshotIncludeIndex`，Leader 已没那段日志 → 改发 `InstallSnapshot` 把整个快照发过去。

> 一句话：**快照是 Raft 解决"日志无限增长"的标准手段**。

---

#### Q3：`prevLogIndex / prevLogTerm` 怎么实现"日志匹配性质"？

##### Log Matching Property（论文 §5.3）

> **如果两节点的日志在 (index, term) 上相同，那么前面所有日志也一定相同。**

##### 直观例子

Leader: `[1(t1) 2(t1) 3(t2) 4(t2) 5(t3)]`

三种 Follower：

```
F1: 1(t1) 2(t1) 3(t2) 4(t2)         ← 落后 1 条
F2: 1(t1) 2(t1) 3(t2)               ← 落后 2 条
F3: 1(t1) 2(t1) 3(t2) 4(★X 错的)    ← 第 4 条不一致
```

Leader 给每个 F 发 AE，带"锚点" `(prevLogIndex, prevLogTerm)`：

```
对 F1：prev=(4, t2)，entries=[E(5,t3)]
        F1 看自己 4 号位：term=t2 ✅ → 接受 → 同步成功
对 F2：prev=(4, t2)，entries=[E(5,t3)]
        F2 没有 4 号位 → 拒绝
        Leader 把 nextIndex[F2]-- → prev=(3, t2)，entries=[D, E]
        F2 看自己 3 号位：term=t2 ✅ → 接受
对 F3：prev=(4, t2)，entries=[E]
        F3 看自己 4 号位：term≠t2 → 拒绝
        Leader nextIndex[F3]-- → prev=(3, t2)，entries=[D, E]
        F3 看自己 3 号位：term=t2 ✅ → 接受 → 覆盖错误的 4 号位
```

##### 为什么"锚点对得上 → 前面就一定全对"？

数学归纳法：

- **基础**：index 1 大家从空开始，必然一致；
- **归纳**：若 (index=k, term=t) 一致 → 这条日志由同一 Leader 在 term=t 时写入 → 那个 Leader 写 k 之前已确认 k-1 一致 → 递推下去前面全对。

依赖两条 Raft 规则：
1. 同一 (term, index) 位置的日志一定相同（同 term 内 Leader 不会改写日志）；
2. Leader 写日志是 append-only。

##### 比喻

把日志想成一条链条。Leader 不需要每节都对，**只要倒数第二节是同一批工厂同一规格的，前面整条就一定一致**——因为那个工厂当时把前面对好了才接的这一节。

##### 一句话

> **`(prevLogIndex, prevLogTerm)` 是 Raft 的"哈希点"——用一对 4 字节比较实现整段日志的一致性保证。** 失败时 `nextIndex--` 一格格回退，直到找到匹配点（工程优化可跳跃式回退，本项目用简单 --）。

---

#### Q4：309 行不等回复就更新 `m_lastResetHearBeatTime`，对吗？

**对，必须不等**。

`m_lastResetHearBeatTime` 表达的是 **"我（Leader）按节奏地发出了一轮 AE"**——是 Leader 自己视角的"上次发出时刻"，不是"广播成功时刻"。

**为什么不能等回复？**

```
5 个 Follower，最慢的 RTT 200ms
若等所有回复才更新：
  doHeartBeat → 等 200ms → 更新 → ticker 醒来再睡 25ms → 下次发
  实际心跳间隔 = 225ms，远超 25ms
  → Follower 选举超时（350ms）逼近 → 大概率把 Leader 选下去
```

**协议上**：心跳节奏（Leader 视角）和回复处理（每个 follower 视角）是**解耦的两件事**。

| 视角 | 关心什么 | 用什么变量 |
| --- | --- | --- |
| Leader | "我下次什么时候再发" | `m_lastResetHearBeatTime`（写完 AE 就更新） |
| Follower | "Leader 还活着吗？" | `m_lastResetElectionTime`（收到 AE 才更新） |

**回复在哪处理？** 在 `sendAppendEntries` 里（每 peer 一个独立线程）：

- 成功 → 推进 `nextIndex[i]` / `matchIndex[i]` / 累加 `*appendNums` → 多数派推进 `commitIndex`；
- 失败 → `nextIndex[i]--`，等下一轮心跳重试。

→ `doHeartBeat` 只管"发"，`sendAppendEntries` 管"收"，**异步解耦**。

---

#### Q5：会不会重复发同一条日志？

**会，而且是有意为之**。Leader 不追踪"哪条已确认"，每次心跳无脑发 `[nextIndex[i] .. lastLogIndex]` 全量。

##### 重发什么时候发生？取决于 RTT vs 心跳间隔

| 场景 | RTT | 是否重发 |
| --- | --- | --- |
| 同机房稳定网络 | < 5ms | **基本不重发**（回复在下一轮心跳前到达） |
| 跨机房 / 公网 | 30~100ms | **必然重发** 1~3 次 |
| 网络抖动 / GC / fsync 卡顿 | 偶发 200ms | 一波重发 8 次 |

> ⚠️ **修正**：之前讲解里说"绝大多数情况下不会重发"，把"处理回复的 CPU 时间（几 ms）"和"RPC 往返时间"混淆了。决定重发的是后者。

##### 为什么重发不出问题？—— Follower 端的幂等性

Follower 处理 AE 时（论文图 2）：

```python
for new_entry in entries:
    target_index = prevLogIndex + 1 + i
    if 我有 target_index 这条日志:
        if 我的[target_index].term == new_entry.term:
            continue                  # 已有 → 跳过（幂等）
        else:
            截断后写入新的            # 冲突 → 用新覆盖
    else:
        写入                          # 没有 → append
```

**关键**："已存在且 term 相同则跳过"——重复发 = 没发，状态不变。

##### Raft 的设计哲学：**正确性优先于性能**

> **Raft 不依赖"少重发"来保证正确性，它依赖"重发也无害"。网络好性能就好，网络差性能就差，但正确性不变。**

##### 减少重发的常见优化（本项目均未实现）

| 优化 | 说明 |
| --- | --- |
| Pipeline 复制 | 不等回复就发下一批 entries（不同内容） |
| Batching | 攒一批客户端请求一次发送 |
| `nextIndex` 跳跃式回退 | Follower 拒绝时回复冲突 term 的最早 index，Leader 直接跳过去 |
| 自适应心跳间隔 | 监控 RTT 动态调整 |

---

#### Q6：每次新建 `appendNums` 不会有很多吗？怎么管理？

**会同时存在 2~3 个，但靠 `shared_ptr` 自动释放，不泄漏不混淆**。

##### 多次心跳 = 多个独立 `appendNums`

```
t=0    心跳 1: appendNums_1（4 个线程持有）
t=25   心跳 2: appendNums_2（4 个线程持有）
t=50   心跳 3: appendNums_3（4 个线程持有）
```

每轮的 `appendNums` 是独立对象，**线程边界把它们隔离开**：

```cpp
auto appendNums = std::make_shared<int>(1);     // 对象 A
std::thread t(&Raft::sendAppendEntries, ..., appendNums);
//                                            ↑
//                  线程 t 永远只改对象 A，不会碰下次的对象 B
```

##### shared_ptr 引用计数自动释放

```
make_shared → 引用计数 = 1
4 个线程持有 → +4，共 5
doHeartBeat 返回 → 局部 appendNums 析构 → -1，剩 4
4 个 sendAppendEntries 线程陆续跑完 → -1 -1 -1 -1，剩 0
→ 自动 delete，内存释放 ✅
```

##### 为什么不复用一个全局 appendNums？

```cpp
std::atomic<int> appendNums;   // ← 不能这么搞
```

会有一堆问题：
- 何时重置为 1？
- 上轮迟到的回复跑到这轮里污染计数怎么办？
- 不同轮次混在一起，多数派阈值意义不明。

**用 shared_ptr 一次性的对象隔离每一轮**——干净、自然、零状态管理。

##### 和 `votedNum` 是同款"轮次令牌"模式

| 特性 | votedNum | appendNums |
| --- | --- | --- |
| 创建 | `doElection` 每轮 | `doHeartBeat` 每轮 |
| 共享者 | 这轮的 N 个 sendRequestVote | 这轮的 N 个 sendAppendEntries |
| 阈值 | n/2+1 | n/2+1 |
| 触发 | 当选 Leader | 推进 commitIndex |
| 释放 | 这轮所有线程跑完自动 | 同上 |

---

#### Q7：Raft 的"三大幂等"设计模式（值得记住）

Raft 在协议每一层都用了幂等性来抵抗网络抖动：

| 场景 | 重复怎么处理 |
| --- | --- |
| 同 term 的 RequestVote 重发 | 投票方查 `votedFor == candidateId` → 再投一次 |
| 同 index 的 AE 重发 | 接收方查 `(index, term)` 已存在 → 跳过 |
| 客户端 Put 重发 | KvServer 查 `(clientId, requestId)` 已处理 → 直接返回上次结果 |

> **核心思想**：把"操作"设计成幂等的，于是网络抖动 / 重发 / 超时重试都不需要专门的恢复代码——重复执行 = 没执行。

这是分布式系统的"内功心法"——TCP、HTTP/2、gRPC、Kafka 全都用了它。

---

#### 调用链

```
leaderHearBeatTicker （每 25ms）
        │
        └── doHeartBeat（持锁）
                ├── 双重身份检查
                ├── 共享 appendNums = 1
                ├── for each peer (i != me):
                │      ├── nextIndex[i] <= lastSnapshotIncludeIndex
                │      │      → 起线程 leaderSendSnapShot
                │      └── 否则
                │             ├── 算 prevLog
                │             ├── 装 entries（两个分支）
                │             ├── 不变量断言
                │             └── 起线程 sendAppendEntries
                └── m_lastResetHearBeatTime = now()
                                                │
                                                ▼
                              sendAppendEntries（异步）
                                ├── 收回复
                                ├── term 比较 / nextIndex 调整
                                ├── *appendNums 累加
                                └── 多数派 → 推进 commitIndex
                                                │
                                                ▼
                                       applierTicker 看到 commitIndex 推进 → apply
```

#### 一句话总结

> **`doHeartBeat` = Leader 给每个 Follower 量身定制一份 AE 并并行发出去 + 喂自己一次心跳狗。心跳和日志复制共用同一条 RPC、同一段代码。靠 `(prevLog, term)` 锚点保证日志匹配性质，靠 `shared_ptr` 当轮次令牌隔离每轮的多数派统计，靠 Follower 端幂等性消化重发——正确性优先于性能。**

#### 疑问 / TODO

- [ ] `getPrevLogInfo` 的实现细节：为什么传指针不是返回值？看 `raft.cpp` 同名函数。
- [ ] `getSlicesIndexFromLogIndex` 的实现：全局 index → 数组下标的映射，会不会有 off-by-one？
- [ ] `leaderSendSnapShot` 的完整流程：什么时候触发本节点 `Snapshot()`？快照格式？InstallSnapshot RPC 字段？下次精读。
- [ ] 给本项目加一个最小版的"`nextIndex` 跳跃式回退"优化：Follower 拒绝时回 `XTerm/XIndex/XLen`，Leader 不用一格格 --。
- [ ] 实现 batching：`KvServer::Start` 攒一批请求再触发心跳，对比吞吐量提升。
- [ ] 故意制造一个会触发"重发"的场景（比如 sleep 让 Follower 处理变慢），观察日志确认幂等分支被命中。
- [ ] 把 `appendNums` 也改成 `shared_ptr<atomic<int>>`，跟 `votedNum` 一并整理（同样的潜在线程安全点）。

---

### 2026-05-30 · `Raft::sendAppendEntries` 大集成 + 基础概念梳理 + Figure 8 反例

- **文件**：`src/raftCore/raft.cpp`
- **行号**：828 ~ 924（含被动方 `AppendEntries1` 8 ~ 132 的对照阅读）
- **关键词**：term 三段比较 / nextIndex 回退 / matchIndex 推进 / Figure 8 / 当前 term commit 限制 / 各种幂等保护

> ⚠️ 这是目前为止最难的一节。读这个函数之前**先把基础概念梳理一遍**，否则代码每行都看得懂、整体却看不懂。

---

## 一、Raft 基础概念再梳理（解决"看不懂"的根源）

### 1) 日志（LogEntry）= "待执行命令"的记录

```cpp
struct LogEntry {
  int index;     // 全局位置（从 1 开始，0 是哨兵）
  int term;      // 写入这条日志时，当时的 Leader 是哪一任
  Op  command;   // 真正的业务命令
};
```

**关键认知**：日志是"待执行"的记录，**不可变**——一旦写入，term 和 command 永远不变（除非被新 Leader 覆盖删除）。

### 2) term = 逻辑时钟

term 从 0 开始，每次新选举 +1，**只增不减**。三个用途：

| 用途 | 说明 |
| --- | --- |
| 选举唯一性 | 每个 term 最多 1 个 Leader |
| 检测过时 | 所有 RPC 带 term，term 小的一方要更新 |
| 日志归属 | 每条日志记录"我是哪一任 Leader 写的" |

### 3) Leader 视角下日志的四个状态

```
索引:    1    2    3    4    5    6    7    8
       ┌──┬──┬──┬──┬──┬──┬──┬──┐
日志:  │T1│T1│T2│T2│T3│T3│T5│T5│
       └──┴──┴──┴──┴──┴──┴──┴──┘
                  ↑       ↑       ↑
           lastApplied commitIndex lastLogIndex
              = 4         = 6        = 8
```

| 区间 | 状态 |
| --- | --- |
| `[1, lastApplied]` | 已 apply 到 SkipList |
| `(lastApplied, commitIndex]` | 已 commit 但未 apply |
| `(commitIndex, lastLogIndex]` | 已写入本地但未 commit（**还可能被新 Leader 覆盖**）|

不变量：`lastApplied ≤ commitIndex ≤ lastLogIndex`。

### 4) commit vs apply

| 概念 | 谁做 | 改什么 | 含义 |
| --- | --- | --- | --- |
| **commit** | Leader 单方面判断 | 推 `m_commitIndex` | "被多数派复制了，永远不丢" |
| **apply** | 每节点后台循环 | 推 `m_lastApplied` + 改 SkipList | "真正执行命令" |

**commit 是下决定，apply 是做执行。** Leader 先 commit，通过 AE 的 `leaderCommit` 字段同步给 Follower，每个节点各自 apply。

### 5) Leader 怎么把日志同步给 Follower

为每个 peer 维护两个数组：

| 数组 | 初值（刚当选） | 含义 |
| --- | --- | --- |
| `m_nextIndex[i]` | `lastLogIndex + 1`（乐观） | 下次给 peer i 发的起始位置 |
| `m_matchIndex[i]` | `0`（保守） | 已知 peer i 复制成功的最大 index |

每 25ms 心跳：构造 prevLog 锚点 + entries → 发 AE → 收回复推进 nextIndex / matchIndex / commitIndex。

### 6) 持久化（论文图 2 强制）

| 字段 | 不持久化的灾难 |
| --- | --- |
| `currentTerm` | 重启后忘了在哪个 term，可能"穿越回过去" |
| `votedFor` | 重启后忘了已投过票，同 term 又投一次 → 两个 Leader |
| `m_logs` | 重启后日志全丢 → 数据丢失 |
| `lastSnapshotInclude*` | 重启后不知道快照覆盖到哪 |

**任何修改这些字段的代码段，结束前必须调 `persist()`**。

### 7) 身份不变量

| 身份 | 不变量 |
| --- | --- |
| Leader | 维护并推进 nextIndex/matchIndex/commitIndex，主动发 AE |
| Candidate | 自投票、不接受同 term 别人的票、等回复 |
| **Follower** | **不主动改 nextIndex/matchIndex/commitIndex**，被动接收 AE |

身份切换瞬间状态字段必须一致——比如退 Follower 必须同时改 status + 重置 votedFor + 持久化。

### 8) `lastSnapshotInclude*` 的 `*` 不是 C++ 指针！

是 markdown 通配符。指 `m_lastSnapshotIncludeIndex` 和 `m_lastSnapshotIncludeTerm` 这一对**普通整数**：

| 字段 | 含义 |
| --- | --- |
| `lastSnapshotIncludeIndex` | 我做过的最后一个快照覆盖到哪个全局 index |
| `lastSnapshotIncludeTerm` | 那条日志的 term（接缝点 term 备份） |

**每个节点独立维护、独立持久化**到 `bin/snapshotPersist<i>.txt`（节点 i 自己的快照文件）+ `raftstatePersist<i>.txt`（这两个 int）。各节点的 lastSnap 通常**不一致**——做快照节奏不同。但同一 index 上记录的 term 必然一致（来自 Log Matching Property + 已 commit 一致性）。

---

## 二、`sendAppendEntries` 函数主体

由 `doHeartBeat` 给每个 peer 起独立线程调用。**职责 = 发一次 AE + 处理回复**。是 Raft 实现里最长最复杂的函数之一。

### 函数全貌

```
1. 发 RPC 等回复
2. 网络层 / 应用层兜底（ok / Disconnected）
3. 加锁
4. term 三段比较
5. 身份兜底（已退 Follower 就别处理了）
6. 失败分支：回退 nextIndex 重试
7. 成功分支：
     ├── 累加 *appendNums
     ├── 用 std::max 单调推进 matchIndex / nextIndex
     ├── 多数派触发 → *appendNums = 0 防重入
     └── 仅当本批 entries 包含当前 term 日志才推进 commitIndex（Figure 8 修复）
```

### 与 `sendRequestVote` 的对照速览

| 维度 | sendRequestVote | sendAppendEntries |
| --- | --- | --- |
| 共享计数器 | `votedNum` | `appendNums` |
| 阈值动作 | 当选 Leader | 推进 commitIndex |
| 失败响应 | 不重试 | 回退 `nextIndex[i]` 等下次 |
| 成功副作用 | 初始化 nextIndex/matchIndex/起心跳 | 推进 nextIndex/matchIndex |
| 安全性核心检查 | 无 | **当前 term commit 限制（Figure 8）** |
| 持久化 | 三变后调 `persist()` | **没调（疑似 bug）** |

---

## 三、14 个具体问题逐条回答

### Q1：`appstate==Disconnected` 返回 ok=true，有影响吗？

**没影响**。`return ok` 是 early return，没动任何 Raft 状态；调用方（detached 线程）也不做什么。但返回 true 语义不严谨，更精确应返回 false。**接口瑕疵，不影响正确性**。

### Q2：846 行不加锁会怎样？

下面要并发读改 `m_status / m_currentTerm / m_votedFor / m_nextIndex / m_matchIndex / m_commitIndex`，不加锁会出现：

- term 撕裂（一半新一半旧的状态被另一线程读走）；
- vector UB（`m_logs` 并发 push_back + 遍历）；
- 身份字段半新半旧。

和 `doHeartBeat` 加锁原因一样。

### Q3：853 行为什么改 `m_votedFor`？这不是投票函数啊！

**因为 votedFor 是"在 currentTerm 这个任期内投给了谁"**。term 变了，votedFor 必须重置为 -1（新 term 还没投过任何人）。否则会留下"votedFor 还指着旧 term 的某人"的脏数据，影响后续投票决策。

> 三变（`status / currentTerm / votedFor`）必须一起改，缺一不可。

### Q4：为什么要持久化？sendRequestVote 调了 persist 这边没调，是 bug 吗？

**是 bug 嫌疑**。

`m_currentTerm / m_votedFor` 都改了，按论文图 2 必须落盘。对照 sendRequestVote 那边（786-788）调了 `persist()`，本函数 850-854 没调——**应该补上**。

不持久化的灾难：见基础概念 6。

### Q5：什么是 Follower 的不变量？

每身份有自己必须维持的状态约束：

- Leader：维护并推进 nextIndex/matchIndex/commitIndex；
- **Follower：不主动改这些工具数组**，只在收到 AE 时通过 `leaderCommit` 被动推进自己的 commitIndex。

861 行身份兜底就是防"已退 Follower 还在执行后半段 Leader 才该做的事"破坏不变量。

### Q6：什么是"临界状态"？（撤回过度推测）

我之前说"Follower term 刚切的临界态"——**撤回，过度推测**。

更准确的说法：`-100` 是 Follower 端的**显式"无建议"哨兵值**——某些早期路径（如 term 不对直接拒绝）根本没走到"算 nextIndex 建议"那一步，就用 -100 占位。读 `AppendEntries1` 的代码（17 行）就能看到："如果 term 太小，回 false 并 set_updatenextindex(-100)"——清晰简单，没有什么"临界态"那么神秘。

### Q7：855-858 reply.term < my 时为什么返回 ok=true、不做处理？

**返回 true**：网络层是通的，只是回复内容过期。
**不处理**：过期回复就是垃圾——

```
t=1   Leader（term=5）发 AE
t=2   Leader 自己 term 升到 8
t=3   t=1 的回复才回来，reply.term=5
      → 用 term=5 时刻的状态推进 term=8 的状态机
      → 数据错乱
```

直接丢弃。

### Q8：870-871 注释"第一个日志肯定匹配的"是什么意思？

意思是 **`nextIndex` 不会无限往前退到 0 或负数**。最坏情况退到 1（prev=0，对应初始空状态——所有节点都"一致"）或退到 `lastSnapshotIncludeIndex+1`（那个位置已被快照确认）。

→ `nextIndex--` 不会越界到非法值。

### Q9：873-875 注释什么意思？

讲一个**理论隐患**：

> reply.updatenextindex 是 Follower 给的"建议"，但建议可能基于过时信息；rpc 延迟下，建议到达 Leader 时 Leader 的状态可能已变；所以不能直接赋值，应该做合理性校验。

代码 878 行就是直接赋值——**作者知道有风险，写在注释里但没改**。

### Q10：nextIndex 越界场景的实际例子

```
Leader 当前：nextIndex[F] = 50，但 Leader 已经被各种回复推到 nextIndex[F] = 81

t=0   Leader 发 AE_1（prevLogIndex=49）给 F
t=5   AE_1 在网络上漂着
t=10  Leader 通过其他方式让 F 追到 lastLogIndex=80，nextIndex[F] = 81
t=30  AE_1 的回复终于回来，updatenextindex=31（基于很久前的状态）
t=30  如果直接赋值：nextIndex[F] 从 81 退回 31 ❌
      → 下次发一大段 F 已有的日志，浪费 + 可能引发错乱
```

修复（应做但本项目没做）：

```cpp
sug = std::clamp(sug, m_lastSnapshotIncludeIndex+1, lastLogIndex+1);
sug = std::max(sug, m_matchIndex[server] + 1);  // 不能退到已确认复制位置之前
m_nextIndex[server] = sug;
```

> **修正**：之前我说"会导致 nextIndex < lastSnapshotIncludeIndex+1"——其实较少发生。真实的隐患主要是**"性能问题（不必要的回退）"**，不是正确性问题。

### Q11：885-886 注释什么意思？

注释在反思"早期错误写法"：

```cpp
// 错误：m_matchIndex[server] += args->entries_size();
//        ↑ 重发会重复加，matchIndex 涨到比真实位置还高
```

887 行的正确写法：

```cpp
m_matchIndex[server] = std::max(m_matchIndex[server],
                                args->prevlogindex() + args->entries_size());
```

用 `max` 单调递增 + 用绝对位置 `prevLogIndex + size` 而不是相对增量——双重幂等保护。

### Q12：891-893 断言为什么？

```cpp
myAssert(m_nextIndex[server] <= lastLogIndex + 1, ...);
```

`nextIndex` 有效范围是 `[1, lastLogIndex+1]`。超过 `lastLogIndex+1` 意味着"要从未来还没写入的位置开始发"——没意义。这是不变量校验。

> 你说"难道只有已经 log 的才能发给 follower 吗"——是的。Leader 不发未来还没写的日志。

### Q13：899-903 注释什么意思？

两条注释：

**注释 1**："不断遍历来统计 rf.commitIndex"——指标准做法是扫描 matchIndex 数组：

```cpp
// 标准做法（本项目没用）：
for N = lastLogIndex; N > m_commitIndex; N-- {
  count = 0;
  for each peer: if matchIndex[peer] >= N: count++
  if count > n/2 && m_logs[N].term == m_currentTerm:
    m_commitIndex = N; break
}
```

本项目简化为：直接用本次 AE 的 `prevLogIndex + entries_size()` 作推进目标。简化但合理——本次 AE 回复刚证明这位置被多数派复制。

**注释 2**：就是 Figure 8 反例的修复规则——见下面 Q14 后的专节。

### Q14：916-918 断言看不懂？有必要吗？

```cpp
myAssert(m_commitIndex <= lastLogIndex, ...);
```

commitIndex 不能超过 lastLogIndex——不能"提交还没写入的日志"。基本不变量。

**正常运行不会触发**。但断言的价值正是"通常不会发生但万一发生立刻定位"——比如算 commitIndex 时 prevLogIndex / entries_size 算错，commitIndex 可能跳到非法值。生产环境下这种 bug 极隐蔽，断言能立刻暴露。

---

## 四、Figure 8 反例 + 当前 term commit 限制（最重要的一节）

### 规则（论文 §5.4.2）

> **Leader 不能直接 commit 一条"过去 term 写的日志"，即使它已被多数派复制。必须等本 term 写一条新日志、并复制到多数派之后，前面那条旧日志才能"顺带"被 commit。**

### 代码体现（908-915 行）

```cpp
if (args->entries_size() > 0
    && args->entries(args->entries_size() - 1).logterm() == m_currentTerm) {
  m_commitIndex = std::max(m_commitIndex,
                           args->prevlogindex() + args->entries_size());
}
```

只有本批 entries 最后一条 term == 当前 term 才推进 commitIndex。

### 完整反例时间线

5 个节点 S1~S5。

**term 2**：S1 当 Leader，写 `2(t2)` 复制到 S1, S2，**还没 commit 就挂了**。

```
S1: [1(t1)] [2(t2)]   (挂)
S2: [1(t1)] [2(t2)]
S3: [1(t1)]
S4: [1(t1)]
S5: [1(t1)]
```

**term 3**：S5 当选 Leader（投票方都还是 t1，可以投给它），写 `2(t3)`，但还没复制就挂了。

```
S5: [1(t1)] [2(t3)]   (挂)
```

**term 4**：S1 重新上线当选，把 `2(t2)` 复制到 S3 → 多数派（S1+S2+S3）。

```
S1: [1(t1)] [2(t2)]   ← Leader (t4)
S2: [1(t1)] [2(t2)]
S3: [1(t1)] [2(t2)]   ← 新复制
```

**关键问题**：S1 看 `2(t2)` 已被多数派复制——能不能 commit？

#### 错误情况：允许 commit `2(t2)`

S1 commit 后告诉客户端"成功"，然后 S1 又挂了。

**term 5**：S5 又当选（lastLogTerm=t3 > t2/t1，符合 Election Restriction）。S5 把自己的 `2(t3)` 强制覆盖：

```
S2: [1(t1)] [2(t3)]  ← 已 commit 的 2(t2) 被覆盖！💥
S3: [1(t1)] [2(t3)]  ← 同上
S5: [1(t1)] [2(t3)]
```

→ **客户端被告知"成功"的写入丢了，安全性破坏**。

#### 修复：term 4 时不能直接 commit `2(t2)`

S1 必须先写 `3(t4)` 复制到多数派，才能顺带 commit `2(t2)`：

```
S1: [1(t1)] [2(t2)] [3(t4)]
S2: [1(t1)] [2(t2)] [3(t4)]
S3: [1(t1)] [2(t2)] [3(t4)]
```

此时 S5 想抢 Leader 必须有 `3(t4)`（lastLogTerm=t4 比 t3 大）——但 S5 没有，**永远当不上**。只有 S1/S2/S3 能当选，它们都包含 `2(t2)` 和 `3(t4)`，不可能覆盖。✅

### 为什么这样能修？

**Election Restriction 只能保护"当前 term 复制到多数派的日志"** 不被未来 Leader 覆盖。对"过去 term 的日志"没有这个保护。

修复办法：先 commit 一条当前 term 的日志，**把过去 term 的日志"绑"在它前面**——借助 Election Restriction 间接保护。

### 实际后果

新 Leader 刚选出来时，commitIndex 可能"卡住"不动，直到客户端写一条新请求（或 Leader 主动写 no-op 日志）触发当前 term 的复制。

**这是 Raft 实现里最反直觉的安全规则**——必须在大脑里建立这个模型。

---

## 五、后续追问的几个修正

### 修正 1：`prevLog` 锚点干什么用

回顾 Log Matching Property：**`(prevLogIndex, prevLogTerm)` 是 Leader 给 Follower 的"对账点"**——只要这一对在两节点上一致，前面所有日志一定也一致。Follower 用 O(1) 比较就能验证整段日志的匹配性。

### 修正 2：term 相同就一定内容相同吗？

**是**。两条铁律推导：
1. 同 term 内最多 1 个 Leader（投票限制）；
2. Leader 写日志是 append-only（不重写）。
→ 同 (index, term) 必由同一 Leader 同一份内容写入。

代码里 `AppendEntries1` 第 82-90 行有 `myAssert(false, "term 相同但 command 不同")` 主动验证这个不变量——比静默错乱好得多。

### 修正 3：Follower 收到 AE 的完整决策树

```
收到 AE
  │
  ├── args.term < m_currentTerm
  │      → 拒绝(success=false, updatenextindex=-100)
  │      → 不喂狗（Leader 过时了）
  │      → return
  │
  ├── DEFER 注册 persist()
  │
  ├── args.term > m_currentTerm
  │      → 三变（status=Follower, term=args.term, votedFor=-1），继续
  │
  ├── (此时 args.term == m_currentTerm)
  │      → 强制 m_status = Follower（Candidate 收到同 term AE 也要变）
  │      → 喂狗：m_lastResetElectionTime = now()
  │
  ├── 检查 prevLogIndex 边界
  │      ├── prevLogIndex > lastLogIndex（我太短了）
  │      │     → updatenextindex = lastLogIndex+1
  │      └── prevLogIndex < lastSnapshotIncludeIndex（建议太老）
  │            → updatenextindex = lastSnapshotIncludeIndex+1
  │
  ├── matchLog 锚点检查
  │      ├── 匹配 ✅
  │      │     ├── 逐条幂等处理 entries
  │      │     │     ├── 超过本地末尾 → push_back
  │      │     │     └── 未超过 → term 不同覆盖、相同跳过
  │      │     └── m_commitIndex = min(leaderCommit, lastLogIndex)
  │      │
  │      └── 不匹配 ❌
  │            → updatenextindex = prevLogIndex（一格回退）
  │
  └── DEFER 触发：persist 后释放锁
```

### 修正 4：两个边界分支何时触发？

| 分支 | 实际场景 |
| --- | --- |
| `prevLogIndex > lastLogIndex` | **最常见**。新 Leader 刚当选时乐观初始化 nextIndex；Follower 长时间宕机回来 |
| `prevLogIndex < lastSnapshotIncludeIndex` | 较少见。InstallSnapshot 和 AE 在网络上乱序到达 |

### 修正 5：S2 几轮才追上 5000 条日志？

**之前说"几轮"不严谨**。修正：

- 本项目无 batching，理论上**1 条 AE 能塞所有 5000 条**；
- 实际通常 **1~2 轮**（第 1 轮"对账失败 + 校准"，第 2 轮"全发"）；
- 但如果遇到 **matchLog 不匹配**（中间有冲突日志），每轮只退 1 格，最坏 5000 轮——**这是优化空间**，论文 §5.3 末尾的"跳跃式回退"还没实现。

### 修正 6：撤回"分区跟着错误 Leader 做快照"的反例

之前那个反例**违反 Raft 安全性，不会发生**。原因：

- 少数派分区不能凑齐多数派 → 不能 commit；
- 不能 commit → 不能 apply；
- 不能 apply → 不能做新快照。

→ "在错误分区跟着错误 Leader 做了 lastSnap=200" 是不可能的。

**真正会触发 `prevLogIndex < lastSnapshotIncludeIndex` 的场景**是 InstallSnapshot 和 AE 在网络上乱序到达：先到的 AE 让 F 追到了某个位置；后到的 InstallSnapshot 把 F 推到更后；然后 Leader 又发了一条基于旧 nextIndex 的 AE，prevLog 就落到了快照范围内。

> **教训**：分布式学习中最危险的是"听起来能讲通但其实违反协议"的伪知识。**多问一句"这真的能发生吗"是好习惯**。

---

## 六、调用链

```
doHeartBeat
   └── 每 peer 一个 std::thread → sendAppendEntries
                                       │
                                       ├── 网络 RPC（无锁）
                                       ├── 加锁
                                       ├── term 三段比较
                                       │     ├── 高 → 退 Follower（缺 persist）
                                       │     ├── 低 → 丢弃
                                       │     └── 等 → 继续
                                       ├── 身份兜底（非 Leader 直接 return）
                                       ├── 失败 → m_nextIndex[server] = updateNextIndex
                                       └── 成功 →
                                            ├── *appendNums + 1
                                            ├── matchIndex/nextIndex 用 max 推进
                                            └── 多数派 →
                                                 ├── *appendNums = 0 防重入
                                                 └── 仅当 entries 末条 term == 当前 → 推进 commitIndex
                                                                                      （Figure 8 修复）
```

## 七、一句话总结

> **`sendAppendEntries` = AE 回复管家：term 三段比较 → 失败回退 nextIndex → 成功更新 matchIndex/nextIndex → 多数派且最后一条是当前 term 时推进 commitIndex。**
>
> **关键安全保证**：
> - **DEFER 让 persist 先于解锁**（防同 term 投两次）
> - **`std::max` 单调推进**（防重发回退）
> - **当前 term commit 限制**（防 Figure 8 反例）

## 八、疑问 / TODO

- [ ] 850-854 行三变后**没调 persist()**，对照 sendRequestVote 786-788 行确认是否疏漏，补上后跑测试。
- [ ] 完整 `AppendEntries1`（被动方）的精读：matchLog 实现、逐条幂等处理 entries 的细节、commitIndex 推进的 `min` 公式。
- [ ] 实现"跳跃式回退"优化：matchLog 不匹配时，Follower 回 `XTerm/XIndex`，Leader 跳过整个冲突 term。预期把"5000 轮"降到"1~2 轮"。
- [ ] 把 Figure 8 反例自己手画一遍（之前 TODO 一直没做）—— 这次总结里完整描述了，但**自己画一遍才能记牢**。
- [ ] 给 `updatenextindex` 加合理性校验（clamp 到 `[lastSnapshotIncludeIndex+1, lastLogIndex+1]`，且 ≥ matchIndex+1）。
- [ ] 验证 `appendNums` 改成 `shared_ptr<atomic<int>>` 后行为不变。
- [ ] 跟踪日志：故意制造"InstallSnapshot 和 AE 乱序到达"的场景（在测试代码里加 sleep 模拟），观察是否真的命中 `prevLogIndex < lastSnapshotIncludeIndex` 分支。

---

### 2026-06-01 · `Raft::AppendEntries1` 被动方处理 AE + 跳跃式回退原理

- **文件**：`src/raftCore/raft.cpp`
- **行号**：8 ~ 152
- **关键词**：被动方 AE / 逐条幂等写入 / 跳跃式回退 / 主动验证 Log Matching / 同 term 唯一 Leader 铁律

> 终于把 `sendAppendEntries`（主动方）的对偶补全。读完这个，**整条日志复制路径完全闭环**。

#### 代码位置

```cpp
void Raft::AppendEntries1(const AppendEntriesArgs* args, AppendEntriesReply* reply);
```

被动方处理 Leader 发来的 AE 请求。对应论文图 2 **AppendEntries 接收方** 5 条规则：

1. Reply false if `term < currentTerm`
2. Reply false if log doesn't contain entry at prevLogIndex with prevLogTerm matching
3. If existing entry conflicts (same index, different term): delete it and all that follow
4. Append new entries
5. If `leaderCommit > commitIndex`, set `commitIndex = min(leaderCommit, lastNew)`

#### 函数决策树

```
收到 AE
  │
  ├── 1) appstate = AppNormal     （乐观初值，对应主动方的悲观 Disconnected）
  │
  ├── 2) args.term < my           → 拒绝(updatenextindex=-100)，return
  │                                 不喂狗（Leader 过时）
  ├── 3) DEFER 注册 persist()     ← 注册位置在第 2 步之后，避免无意义 persist
  │
  ├── 4) args.term > my           → 三变（status/term/votedFor），不 return
  │
  ├── 5) m_status = Follower       ← 强制（防 Candidate 没退）
  │     m_lastResetElectionTime = now()  ← 喂狗
  │
  ├── 6) prevLogIndex 边界检查
  │     ├── > lastLogIndex                  → 建议 lastLogIndex+1，return
  │     └── < lastSnapshotIncludeIndex      → 建议 lastSnapInclude+1
  │                                            ⚠️ 此分支似乎漏 return
  │
  ├── 7) matchLog(prevLogIndex, prevLogTerm)
  │     ├── 匹配 ✅
  │     │     ├── 逐条幂等处理 entries（不直接截断）
  │     │     ├── commitIndex = min(leaderCommit, lastLogIndex)
  │     │     └── reply success=true
  │     │
  │     └── 不匹配 ❌
  │           ├── 跳跃式回退：往前找冲突 term 的第一个 index
  │           ├── reply success=false
  │           └── return
  │
  └── DEFER 触发：persist() 后释放锁
```

#### 关键设计点

##### A. 主动 / 被动的 appstate 双向初值

```cpp
// 主动方（doHeartBeat）：set_appstate(Disconnected)  ← 悲观
// 被动方（AppendEntries1）：set_appstate(AppNormal)  ← 乐观
```

被动方"我能收到代表网络是通的"——能进函数就乐观假设网络 OK。这是分布式编程里"双向悲观/乐观"的常见模式。

##### B. DEFER persist 的位置极其讲究

注册在 14-21 行（term 太小早 return）**之后**：

| 位置 | 后果 |
| --- | --- |
| 14 行之前注册 | term 太小那个分支也会无意义 persist |
| **14 行之后注册（当前）** | term 太小早 return 不 persist；其他所有路径 ✅ |

一次注册，DEFER 自动覆盖后面所有 return 路径。

##### C. 强制 `m_status = Follower`（38 行）

```cpp
m_status = Follower;  // 这里是有必要的，因为如果 candidate 收到同一个 term 的 leader 的 AE，需要变成 follower
```

场景：

```
本节点是 term=5 的 Candidate
收到同 term=5 的 AE
→ 25 行 if (args.term > my) 不成立
→ 但同 term 已选出 Leader，必须退选回 Follower
```

→ 这一行专门给 Candidate 退选用。**喂狗在它之后**——这就是上一节讲的"喂狗 = 给进行中的进展（被服务/等候选人/等自己）留时间"。

##### D. 逐条幂等写入 entries（71-96）

**绝对不能写成"截断后追加"**：

```cpp
// ❌ 错误：会丢数据
m_logs.resize(prevLogIndex + 1);
m_logs.insert(m_logs.end(), entries.begin(), entries.end());
```

反例：

```
F 已有 [E5, E6, E7]，lastLogIndex=7
重发的 AE 来了：prevLogIndex=4，entries=[E5, E6]
如果"截断后追加"：m_logs.resize(5) → 把 E7 删了！💥
```

源码注释 67 行明确说："**不能直接截断，必须一个一个检查**"。

正确做法：逐条幂等：

```cpp
for (auto& log : entries) {
  if (log.index > lastLogIndex)        m_logs.push_back(log);
  else if (term 不同)                   覆盖那一条
  // term 相同 → 跳过（幂等，重发也无害）
}
```

##### E. 主动验证 Log Matching Property（82-90）

```cpp
if (m_logs[...].logterm() == log.logterm()
    && m_logs[...].command() != log.command()) {
  myAssert(false, "term 相同但 command 不同！");
}
```

**Log Matching Property**：同 (index, term) ⟹ command 必然相同。这个 assert 主动验证不变量——比静默错乱好得多。

> ⚠️ 79 行有 todo 注释："这个地方放出来会出问题"——作者在测试中发现 assert 会触发，原因未查清（可能是某些边界 bug 或 protobuf 序列化差异）。**疑点未解决**。

##### F. commitIndex 同步用 `min`（109-110）

```cpp
m_commitIndex = std::min(args->leadercommit(), getLastLogIndex());
```

为什么不直接 `m_commitIndex = leaderCommit`？

```
Leader 已 commit 到 100，但 F 自己只到 80（还没补齐）
m_commitIndex = 100 → 越过自己 lastLogIndex → 后续 apply 越界
m_commitIndex = min(100, 80) = 80 ✅ 等下次 AE 补齐再推进
```

##### G. 跳跃式回退（126-149）—— **本项目实现了简化版**

```cpp
reply->set_updatenextindex(args->prevlogindex());  // 默认值

for (int index = args->prevlogindex(); index >= m_lastSnapshotIncludeIndex; --index) {
  if (getLogTermFromLogIndex(index) != getLogTermFromLogIndex(args->prevlogindex())) {
    reply->set_updatenextindex(index + 1);
    break;
  }
}
```

往前扫描，找到"和 prev 位置 term 不同的位置"，让 Leader 跳过整个 F 的冲突 term 段。**比 nextIndex-- 一格格退快 N 倍**（N=冲突 term 的长度）。

> 上一节讲 sendAppendEntries 时我说"本项目没实现跳跃式回退"——**修正**：实现了简化版，只是不是论文 §5.3 的完整版。

---

#### 跳跃式回退的算法原理

##### 核心观察

不匹配意味着 F 在 prevLogIndex 位置的 term ≠ Leader 期待的 prevTerm：

```
Leader: prev=(5, t1)        ← 期待 t1
F:      m_logs[5].term = t2  ← 实际 t2
```

**F 的整个 t2 段大概率都是过期日志**——某个曾经的 t2 Leader 写完没 commit 就挂了，新 Leader 接管后改写。

##### 算法

往前扫描找"term 改变的位置"：

```
F 日志：[1(t1) 2(t1) 3(t2) 4(t2) 5(t2) 6(t3)]
                              ↑ prev=5
往前扫：
  index=5: t2 ✓
  index=4: t2 ✓
  index=3: t2 ✓
  index=2: t1 ← 不同 term！
  → updatenextindex = 3（跳过整个 t2 段）
```

下次 Leader 用 prev=(2, t1) 试，匹配 → 把 [3, 4, 5] 整段覆盖。

##### "不一定都矛盾"是什么意思？（修正之前讲法）

源码 128-130 行注释："为什么该 term 的日志都是矛盾的呢？也不一定都是矛盾的，只是这么优化减少 rpc 而已"。

**修正之前一个错误**：我前面讲过"旧 t2 Leader 写完后被新 t2 Leader 接力"——**这违反 Raft 铁律**。

> **铁律：每次新选举必然 `currentTerm++`，所以同 term 内最多一个 Leader，不存在"接力"。**

真实场景：F 的冲突 term 段里**可能某些位置碰巧和 Leader 一致**——算法激进地把整段都让 Leader 重发，最坏多一次 RPC，没有正确性问题。

#### Q&A 集

**Q1：F 比 Leader 更长怎么办？**

会发生（旧 Leader 写完没 commit 就挂、新 Leader 上任时日志比 F 短）。代码处理：

- 71-96 行只遍历 entries 范围内的位置；
- F 多出来的部分**保留不动**（但都是 uncommitted，不会 apply）；
- 未来某轮 AE 会用 Leader 的真正版本覆盖。

> **注意**：论文 §5.3 严格要求"删除冲突 entry 及其之后的所有"，本项目实现是"逐条覆盖范围内位置"，**严格度略低**——但因为多出的部分都 uncommitted，不影响正确性。

**Q2：61 行 `else if (prevLogIndex < lastSnapshotIncludeIndex)` 分支没 return，是 bug 吗？**

**疑似 bug**。代码注释里有 `//  return`（被注释掉了）。少 return 会让控制流继续走 `matchLog`，可能在已快照位置上做无效查询。**强建议加上 return** 验证。

**Q3：114-117 行的 assert 是什么？**

```cpp
myAssert(getLastLogIndex() >= m_commitIndex, ...);
```

基本不变量：commitIndex 不能超过 lastLogIndex。前面 109-110 行用 `min` 已经保证，这里 assert 是防 corner case 算错的兜底。

**Q4：141-148 行的 todo "找到符合的 term 的最后一个" 实现了吗？怎么优化？**

**没实现**。这指论文 §5.3 末尾的**完整版跳跃式回退**——双端协同：

```
Follower 回 XTerm/XIndex/XLen：
  XTerm  = F 在 prevLogIndex 位置的 term
  XIndex = F 自己 XTerm 段的第一个 index
  XLen   = F 的日志长度

Leader 收到后：
  如果 Leader 自己有 XTerm → nextIndex = Leader 自己 XTerm 的最后一个 index + 1
  如果没有              → nextIndex = XIndex（跳过 F 的整个 XTerm 段）
  XTerm = -1（F 太短）  → nextIndex = XLen
```

需要给 protobuf 加 `xterm/xindex/xlen` 三个字段，双端代码都要改。

**收益**：在"双方都有冲突 term"场景下更优——直接跳到 Leader 的对应位置，而不是跳到 F 的 term 之前再发。

---

#### 主动方 / 被动方对偶大全

| 维度 | 主动方 sendAppendEntries | 被动方 AppendEntries1 |
| --- | --- | --- |
| appstate 初值 | Disconnected（悲观） | AppNormal（乐观） |
| `term > my` | 三变后 return | 三变后**继续往下** |
| `term < my` | 直接丢弃 | 拒绝 + 把 my term 写回 reply |
| 持久化 | 三变后调 persist（**疑似漏调**） | DEFER 注册一次保所有路径 |
| 失败响应 | `nextIndex = updatenextindex`（直接赋值，无校验） | 设置 updatenextindex 给主动方建议 |
| 成功响应 | 推进 matchIndex/nextIndex/commitIndex | 写入 entries + 推进 commitIndex |
| 安全断言 | `commitIndex ≤ lastLogIndex` | `lastLogIndex >= commitIndex`、`lastLogIndex >= prev+size` |
| 喂狗 | 不喂（主动方的事） | term 合法时喂选举狗 |

#### 关键铁律（由本节质疑诞生）

> **每次新选举必然 `currentTerm++`** → **同 term 内最多一个 Leader** → **不存在"同 term 接力"**。
>
> 任何反例描述里出现"同一个 term 内两个 Leader 接力"都是违反协议的伪知识，必须警惕。

#### 调用链

```
Leader.sendAppendEntries(args, reply)
   │
   └─ RPC ─► Follower.AppendEntries1(args, reply)
                  │
                  ├── 加锁 + appstate=AppNormal
                  ├── term 三段比较
                  ├── DEFER persist
                  ├── 强制变 Follower + 喂狗
                  ├── prevLog 边界检查
                  ├── matchLog
                  │     ├── 匹配 → 逐条幂等写入 + commitIndex 同步
                  │     └── 不匹配 → 跳跃式回退建议
                  └── DEFER 触发：persist 后释放锁
```

#### 一句话总结

> **`AppendEntries1` = Follower 处理 AE 的核心：term 比较 → prev 锚点边界检查 → matchLog 锚点检查 → 匹配则逐条幂等写入 + 同步 commitIndex；不匹配则跳跃式回退建议 Leader 重试。**
>
> **核心安全保证**：
> - **DEFER 注册位置在 term-too-small 之后**（防无意义 persist）
> - **逐条幂等写入而非截断追加**（防重发覆盖新日志）
> - **`min(leaderCommit, lastLogIndex)`**（防 commitIndex 跑到未来）
> - **简化版跳跃式回退**（一次 RPC 跳过整个冲突 term）
> - **主动 assert Log Matching Property**（违反不变量立刻 panic）

#### 疑问 / TODO

- [ ] **53-62 行漏 return 的 bug 嫌疑**：`prevLogIndex < lastSnapshotIncludeIndex` 分支没 return，加上后跑测试。
- [ ] **79 行 todo 调查**：作者说"放出 assert 会出问题"——找出实测会触发的具体场景，是协议 bug 还是误报。
- [ ] **完整版跳跃式回退**：给 reply 加 `xterm/xindex/xlen`，Leader 端协同决策（论文 §5.3）。
- [ ] **F 比 Leader 长的边界**：本项目"保留多余部分"和论文"删除冲突及之后"的差异，跑 6.824 完整测试看是否触发问题。
- [ ] 写一个最小可复现 demo：故意把 71-96 行改成 "shrinkLogsToIndex + append"，观察会不会丢数据——验证幂等设计的必要性。
- [ ] 验证 38 行强制 `m_status = Follower`：写测试让 Candidate 收到同 term AE，确认状态切换正确。
- [ ] 把"同 term 不可能两个 Leader" 的反证写成一段独立笔记，警惕未来再编出违反这个铁律的伪反例。

---

### 2026-06-07 · `Persister` 持久化文件读写器

- **文件**：`src/raftCore/Persister.cpp`
- **头文件**：`src/raftCore/include/Persister.h`
- **行号**：8 ~ 121
- **关键词**：Raft 持久化 / snapshot / 文件流 / mutex / flush vs close / 日志压缩触发

#### 代码位置

```cpp
class Persister {
 public:
  void Save(std::string raftstate, std::string snapshot);
  std::string ReadSnapshot();
  void SaveRaftState(const std::string& data);
  long long RaftStateSize();
  std::string ReadRaftState();
  explicit Persister(int me);
  ~Persister();
};
```

`Persister` 是本项目的**底层落盘工具类**。它不理解 Raft 协议，也不理解 KV 数据库，只负责两类字符串的读写：

| 文件 | 内容 | 上层来源 |
| --- | --- | --- |
| `raftstatePersist<i>.txt` | `currentTerm / votedFor / logs / snapshot 元信息` | `Raft::persistData()` |
| `snapshotPersist<i>.txt` | KV 状态机快照 | `KvServer::MakeSnapShot()` |

一句话：**Raft / KvServer 决定保存什么，Persister 只负责把字符串写进文件、从文件读回来。**

---

#### 推荐阅读顺序

源码里函数顺序是 `Save -> ReadSnapshot -> SaveRaftState -> ... -> 构造函数`，但学习时更适合按生命周期读：

1. `Persister::Persister()`：文件名、文件初始化、输出流绑定。
2. `Persister::~Persister()`：关闭文件流。
3. `clearRaftState()` / `clearSnapshot()` / `clearRaftStateAndSnapshot()`：写入前如何清空旧内容。
4. `SaveRaftState()` / `ReadRaftState()`：普通 Raft 状态持久化。
5. `Save()` / `ReadSnapshot()`：快照场景下同时保存 Raft 状态和 KV 快照。
6. `RaftStateSize()`：供 KVServer 判断是否需要触发快照。

---

#### 逐函数解读

##### 1) `Persister::Persister`：为每个节点准备两个持久化文件（62-90）

```cpp
Persister::Persister(const int me)
    : m_raftStateFileName("raftstatePersist" + std::to_string(me) + ".txt"),
      m_snapshotFileName("snapshotPersist" + std::to_string(me) + ".txt"),
      m_raftStateSize(0) {
```

- `me` 是当前节点编号。
- 节点 0 对应：
  - `raftstatePersist0.txt`
  - `snapshotPersist0.txt`
- `m_raftStateSize` 用于记录 raft state 字符串大小，后面触发 snapshot 时会用到。

```cpp
bool fileOpenFlag = true;
```

记录两个文件是否都能正常打开。只要有一个失败，就置为 `false`。

```cpp
std::fstream file(m_raftStateFileName, std::ios::out | std::ios::trunc);
```

打开 raft state 文件，并用 `trunc` 清空旧内容。

```cpp
if (file.is_open()) {
  file.close();
} else {
  fileOpenFlag = false;
}
```

检查 raft state 文件是否打开成功。这里的 `file` 只是临时检查/清空用，检查完立即关闭。

```cpp
file = std::fstream(m_snapshotFileName, std::ios::out | std::ios::trunc);
```

同样处理 snapshot 文件。

```cpp
if (file.is_open()) {
  file.close();
} else {
  fileOpenFlag = false;
}
```

检查 snapshot 文件是否打开成功。

```cpp
if (!fileOpenFlag) {
  DPrintf("[func-Persister::Persister] file open error");
}
```

如果任意一个文件打开失败，打印调试日志。

```cpp
m_raftStateOutStream.open(m_raftStateFileName);
m_snapshotOutStream.open(m_snapshotFileName);
```

正式打开两个输出流，后续 `SaveRaftState()` / `Save()` 会通过它们写文件。

**注意点**：构造函数会清空旧持久化文件。这对教学/测试启动很方便，但对真实“宕机恢复”不友好。真实持久化恢复时，构造 `Persister` 不应该清空已有文件。

---

##### 2) `Persister::~Persister`：释放文件流资源（92-99）

```cpp
Persister::~Persister() {
```

对象销毁时执行。

```cpp
if (m_raftStateOutStream.is_open()) {
  m_raftStateOutStream.close();
}
```

如果 raft state 输出流还开着，就关闭。

```cpp
if (m_snapshotOutStream.is_open()) {
  m_snapshotOutStream.close();
}
```

如果 snapshot 输出流还开着，也关闭。

这是 RAII 思想：对象负责管理资源，析构时自动释放。

---

##### 3) `clearRaftState`：清空 raft state 文件（101-109）

```cpp
void Persister::clearRaftState() {
```

清空 raft state 文件，并重置状态大小。

```cpp
m_raftStateSize = 0;
```

因为旧 raft state 即将被覆盖，大小先归零。

```cpp
if (m_raftStateOutStream.is_open()) {
  m_raftStateOutStream.close();
}
```

关闭当前输出流。后面要以 `trunc` 模式重新打开文件。

```cpp
m_raftStateOutStream.open(m_raftStateFileName, std::ios::out | std::ios::trunc);
```

重新打开并清空文件。这样下一次写入就是覆盖式写入，不会追加到旧内容后面。

---

##### 4) `clearSnapshot`：清空 snapshot 文件（111-116）

```cpp
void Persister::clearSnapshot() {
```

清空 snapshot 文件。

```cpp
if (m_snapshotOutStream.is_open()) {
  m_snapshotOutStream.close();
}
```

关闭当前 snapshot 输出流。

```cpp
m_snapshotOutStream.open(m_snapshotFileName, std::ios::out | std::ios::trunc);
```

重新打开并清空 snapshot 文件。

---

##### 5) `clearRaftStateAndSnapshot`：同时清空两个文件（118-121）

```cpp
void Persister::clearRaftStateAndSnapshot() {
  clearRaftState();
  clearSnapshot();
}
```

组合函数，主要供 `Save()` 使用。因为快照场景必须同时保存：

```text
新的 raft state + 新的 snapshot
```

---

##### 6) `SaveRaftState`：只保存 Raft 状态（35-41）

```cpp
void Persister::SaveRaftState(const std::string &data) {
```

`data` 是 `Raft::persistData()` 生成的字符串，里面包含：

```text
currentTerm
votedFor
lastSnapshotIncludeIndex
lastSnapshotIncludeTerm
logs
```

```cpp
std::lock_guard<std::mutex> lg(m_mtx);
```

加锁。保护文件流和 `m_raftStateSize`，避免多个线程同时读写 Persister。

```cpp
clearRaftState();
```

清空旧 raft state。

```cpp
m_raftStateOutStream << data;
```

把新的 raft state 字符串写入文件。

```cpp
m_raftStateSize += data.size();
```

更新内存里记录的 raft state 大小。因为 `clearRaftState()` 已经把它置 0，这里 `+=` 实际等价于：

```cpp
m_raftStateSize = data.size();
```

普通持久化链路：

```text
Raft::persist()
  -> Raft::persistData()
  -> Persister::SaveRaftState(data)
```

---

##### 7) `ReadRaftState`：读取 Raft 状态（49-60）

```cpp
std::string Persister::ReadRaftState() {
```

从 `raftstatePersist<i>.txt` 读取 raft state 字符串。

```cpp
std::lock_guard<std::mutex> lg(m_mtx);
```

加锁，避免读取时其他线程正在清空或写入。

```cpp
std::fstream ifs(m_raftStateFileName, std::ios_base::in);
```

打开 raft state 文件用于读取。

```cpp
if (!ifs.good()) {
  return "";
}
```

文件不可读时返回空字符串。

```cpp
std::string snapshot;
```

变量名叫 `snapshot`，但这里实际存的是 raft state。命名不太准确。

```cpp
ifs >> snapshot;
```

从文件读出内容。

**注意点**：`operator >>` 会按空白字符截断，只读取第一个 token。Boost `text_oarchive` 的序列化结果可能包含空格/换行，所以这里有读不完整的风险。

```cpp
ifs.close();
return snapshot;
```

关闭输入流，返回读取结果。

恢复链路：

```text
Raft::init()
  -> Persister::ReadRaftState()
  -> Raft::readPersist(data)
```

---

##### 8) `Save`：同时保存 Raft 状态和快照（8-14）

```cpp
void Persister::Save(const std::string raftstate, const std::string snapshot) {
```

快照场景下调用：既要保存 raft state，又要保存 KV snapshot。

```cpp
std::lock_guard<std::mutex> lg(m_mtx);
```

加锁，保证两个文件的清空和写入是一个连续过程。

```cpp
clearRaftStateAndSnapshot();
```

同时清空旧 raft state 和旧 snapshot。

```cpp
m_raftStateOutStream << raftstate;
```

写入新的 raft state。

```cpp
m_snapshotOutStream << snapshot;
```

写入新的 snapshot。

典型调用点：

```text
Raft::Snapshot(index, snapshot)
Raft::InstallSnapshot(args, reply)
```

为什么 snapshot 要和 raft state 一起保存？

```text
snapshot 保存状态机数据
raft state 保存 snapshot 对应的 lastSnapshotIncludeIndex / lastSnapshotIncludeTerm 和剩余 logs
```

如果只保存 snapshot，不保存新的 raft state，重启后就不知道“快照覆盖到哪条日志”。

**注意点**：`Save()` 没有更新 `m_raftStateSize`，和 `SaveRaftState()` 行为不一致，后面 `RaftStateSize()` 可能不准确。

---

##### 9) `ReadSnapshot`：读取快照（16-33）

```cpp
std::string Persister::ReadSnapshot() {
```

读取 `snapshotPersist<i>.txt`。

```cpp
std::lock_guard<std::mutex> lg(m_mtx);
```

加锁，防止读取时其他线程正在写 snapshot。

```cpp
if (m_snapshotOutStream.is_open()) {
  m_snapshotOutStream.close();
}
```

如果 snapshot 输出流还开着，先关闭。这里解决的不是并发问题，而是**当前对象自己的文件流状态问题**：

- `lock_guard`：防止别的线程同时操作 Persister。
- `close()`：让当前写流 flush 缓冲区并释放对文件的持有，后面再用输入流读同一个文件。

```cpp
DEFER {
  m_snapshotOutStream.open(m_snapshotFileName);  //默认是追加
};
```

函数返回前重新打开 snapshot 输出流，恢复 Persister 后续写 snapshot 的能力。

这里用到了项目里的 `DEFER` 工具：离开当前作用域时自动执行。

```cpp
std::fstream ifs(m_snapshotFileName, std::ios_base::in);
```

打开 snapshot 文件用于读取。

```cpp
if (!ifs.good()) {
  return "";
}
```

如果文件不可读，返回空字符串。因为前面注册了 `DEFER`，即使这里提前 return，输出流也会被重新打开。

```cpp
std::string snapshot;
ifs >> snapshot;
```

读取 snapshot 内容。

同样有 `>>` 的截断风险：snapshot 是序列化字符串，可能包含空白字符，理论上应该读取整个文件。

```cpp
ifs.close();
return snapshot;
```

关闭输入流并返回 snapshot。

使用场景：

```text
1. KvServer 启动时恢复状态机
2. Leader 给落后 Follower 发送 InstallSnapshot RPC
```

---

##### 10) `RaftStateSize`：返回 raft state 大小（43-47）

```cpp
long long Persister::RaftStateSize() {
```

返回当前记录的 raft state 大小。

```cpp
std::lock_guard<std::mutex> lg(m_mtx);
```

加锁，避免读取 `m_raftStateSize` 时另一个线程正在更新。

```cpp
return m_raftStateSize;
```

返回大小。

它会被 `KvServer::IfNeedToSendSnapShotCommand()` 间接使用：

```text
raft state 太大
  -> KvServer 生成 snapshot
  -> Raft::Snapshot() 截断日志
  -> Persister::Save(raftstate, snapshot)
```

---

#### Q&A 集

**Q1：`ReadSnapshot()` 已经加锁了，为什么还要关闭 `m_snapshotOutStream`？**

它们解决的是两个不同问题：

- `std::lock_guard<std::mutex>`：防止其他线程同时读写 Persister。
- `m_snapshotOutStream.close()`：处理当前对象内部的文件流状态，flush 缓冲区并释放写流对文件的持有。

锁只管“别人别动”，不负责把输出流里的缓冲数据刷到文件。

**Q2：那直接 `flush()` 不就行了吗，为什么一定要 `close()`？**

如果目的只是刷新缓冲区，`flush()` 通常就够：

```cpp
m_snapshotOutStream.flush();
```

源码选择 `close()` 是一种更保守、更粗暴但容易理解的做法：

```text
写流彻底结束
-> 重新开输入流读取
-> 函数退出前再打开写流
```

严格说，它不是唯一正确写法。更清爽的实现是：不要长期持有输出流，每次读写都用局部 `ifstream/ofstream`，用完自动关闭。

**Q3：`ReadRaftState()` 里变量为什么叫 `snapshot`？**

命名不准确。这个变量实际保存的是 raft state 字符串，不是 snapshot。建议改名为 `raftState`。

**Q4：为什么 `Save()` 要同时保存 raft state 和 snapshot？**

因为 snapshot 不只是“KV 数据”。Raft 还必须同步保存快照边界：

```text
lastSnapshotIncludeIndex
lastSnapshotIncludeTerm
截断后的剩余 logs
```

否则重启后 KV 数据虽然在，但 Raft 不知道日志从哪里继续匹配。

**Q5：构造函数清空文件，会不会导致持久化恢复失效？**

会。当前代码更偏教学/测试启动方式。真实宕机恢复场景下，构造 `Persister` 时不能用 `trunc` 清空旧文件，否则 `Raft::init()->ReadRaftState()` 读到的永远是空状态。

---

#### 调用链

##### 普通 Raft 持久化

```text
Raft 中 currentTerm / votedFor / logs 变化
  -> Raft::persist()
  -> Raft::persistData()
  -> Persister::SaveRaftState(data)
  -> raftstatePersist<i>.txt
```

##### Raft 状态恢复

```text
KvServer 构造
  -> new Persister(me)
  -> Raft::init(..., persister, ...)
  -> persister->ReadRaftState()
  -> Raft::readPersist(data)
```

##### 本机主动做快照

```text
KvServer::IfNeedToSendSnapShotCommand()
  -> KvServer::MakeSnapShot()
  -> Raft::Snapshot(index, snapshot)
  -> Persister::Save(persistData(), snapshot)
```

##### 接收 Leader 快照

```text
Raft::InstallSnapshot(args, reply)
  -> 截断日志 + 更新 lastSnapshotInclude*
  -> Persister::Save(persistData(), args->data())
  -> ApplyMsg 推给 KvServer
  -> KvServer::ReadSnapShotToInstall()
```

##### Leader 发送快照

```text
Raft::leaderSendSnapShot(server)
  -> m_persister->ReadSnapshot()
  -> InstallSnapshot RPC
```

---

#### 可以优化的地方

1. **不要在构造函数中清空已有持久化文件。**

真实 crash recovery 场景需要保留旧文件。可以把“清空持久化文件”做成测试专用接口，而不是构造函数默认行为。

2. **读取文件时不要用 `ifs >> data`。**

`>>` 会遇到空白字符停止。更稳妥的方式是读取整个文件：

```cpp
std::string data((std::istreambuf_iterator<char>(ifs)),
                 std::istreambuf_iterator<char>());
```

3. **`Save()` 应该同步更新 `m_raftStateSize`。**

当前 `SaveRaftState()` 会更新 size，但 `Save()` 不更新。快照后再调用 `RaftStateSize()` 可能拿到不准确的大小。

4. **可以改成局部文件流，减少长期持有流的复杂度。**

当前类长期持有 `m_raftStateOutStream / m_snapshotOutStream`，所以 `ReadSnapshot()` 才需要 close + DEFER reopen。更简单的写法是每次读写时局部打开：

```cpp
std::ofstream ofs(file, std::ios::out | std::ios::trunc);
ofs << data;
```

5. **补充错误处理。**

当前写入失败、打开失败只做了有限日志，读写失败也没有向上层传播。可以增加返回值或异常处理，避免“持久化失败但 Raft 继续运行”。

6. **统一序列化和落盘格式。**

目前上层用 Boost / Protobuf 混合序列化，Persister 又按普通文本写入。后续可以考虑用二进制安全的文件格式，避免空白字符读取问题。

---

#### 一句话总结

> **`Persister` = 两个文件的线程安全字符串存取器；普通 persist 只写 raft state，snapshot 场景同时写 raft state + KV snapshot。当前实现能帮助理解流程，但在真实持久化恢复、二进制安全读取、size 统计和文件流管理上都有优化空间。**

#### 疑问 / TODO

- [ ] 验证 `ReadRaftState()` / `ReadSnapshot()` 用 `>>` 是否会截断 Boost 序列化结果。
- [ ] 把读取逻辑改成 `istreambuf_iterator` 读取整个文件，跑一次 raft 示例。
- [ ] 修改 `Save()`，让它也更新 `m_raftStateSize = raftstate.size()`。
- [ ] 把构造函数的 `trunc` 清空行为移到测试初始化流程中，模拟真正的 crash recovery。
- [ ] 尝试把长期持有输出流改成“每次读写局部打开文件流”，观察代码是否更简单。

---

### 2026-06-07 · `raft.cpp` 持久化链路总览

- **文件**：`src/raftCore/raft.cpp`
- **相关行号**：8 ~ 24、420 ~ 475、528 ~ 560、603 ~ 609、746、947 ~ 976、988 ~ 1122
- **关键词**：Raft 持久化 / readPersist / persistData / Snapshot / InstallSnapshot / matchIndex / nextIndex / KVServer 状态机

#### 代码位置

本节不是单个函数，而是 `raft.cpp` 中所有和持久化、快照、恢复相关函数的关系梳理。

核心函数按学习顺序：

| 顺序 | 函数 | 行号 | 作用 |
| --- | --- | --- | --- |
| 1 | `Raft::persistData()` | 1049 ~ 1063 | 把 Raft 内存状态序列化成字符串 |
| 2 | `Raft::readPersist()` | 1065 ~ 1085 | 从字符串恢复 Raft 内存状态 |
| 3 | `Raft::persist()` | 603 ~ 609 | 普通 Raft 状态落盘入口 |
| 4 | `Raft::init()` | 988 ~ 1047 | 启动时读取持久化状态 |
| 5 | `Raft::Snapshot()` | 1087 ~ 1122 | 本节点主动生成快照并截断日志 |
| 6 | `Raft::InstallSnapshot()` | 420 ~ 475 | Follower 接收 Leader 快照 |
| 7 | `Raft::leaderSendSnapShot()` | 528 ~ 560 | Leader 读取 snapshot 并发给落后节点 |
| 8 | `Raft::GetRaftStateSize()` | 746 | 给 KVServer 查询 raft state 大小 |

---

#### 总体关系图

```mermaid
flowchart TD
    A[客户端 Put/Get/Append] --> B[KvServer]
    B --> C[Raft::Start<br/>Leader 追加日志]
    C --> D[Raft::persist]
    D --> E[Raft::persistData]
    E --> F[Persister::SaveRaftState<br/>保存 raftstate]

    G[Raft::init 启动] --> H[Persister::ReadRaftState]
    H --> I[Raft::readPersist<br/>恢复 term/vote/log/snapshot边界]

    B --> J[日志 apply 到状态机]
    J --> K{RaftStateSize<br/>是否过大}
    K -->|否| J
    K -->|是| L[KvServer::MakeSnapShot<br/>生成状态机快照]
    L --> M[Raft::Snapshot(index, snapshot)]
    M --> N[截断 m_logs<br/>更新 snapshotIndex/snapshotTerm]
    N --> O[Raft::persistData]
    O --> P[Persister::Save<br/>同时保存 raftstate + snapshot]

    Q[Leader 发现 Follower 太落后<br/>nextIndex <= snapshotIndex] --> R[Raft::leaderSendSnapShot]
    R --> S[Persister::ReadSnapshot]
    S --> T[InstallSnapshot RPC]

    T --> U[Follower::InstallSnapshot]
    U --> V[更新 snapshot 边界<br/>截断日志]
    V --> W[Persister::Save<br/>保存 raftstate + snapshot]
    U --> X[ApplyMsg Snapshot]
    X --> Y[KvServer::ReadSnapShotToInstall<br/>恢复 KV 状态机]
```

图里的三条线要分清：

| 线 | 负责内容 |
| --- | --- |
| `persistData / readPersist` | Raft 自己的共识状态：term、vote、logs、snapshot 元信息 |
| `MakeSnapShot / ReadSnapShotToInstall` | KVServer 的状态机内容：SkipList 数据、客户端去重表 |
| `Persister` | 只负责把字符串读写到文件 |

---

#### 分函数解读

##### 1) `Raft::persistData()`：把 Raft 状态打包成字符串（1049-1063）

```cpp
std::string Raft::persistData() {
```

返回序列化后的 raft state 字符串，最终会交给 `Persister` 写入文件。

```cpp
BoostPersistRaftNode boostPersistRaftNode;
```

创建一个中间对象。`BoostPersistRaftNode` 是 `Raft` 内部类，只承载需要持久化的字段。

```cpp
boostPersistRaftNode.m_currentTerm = m_currentTerm;
boostPersistRaftNode.m_votedFor = m_votedFor;
boostPersistRaftNode.m_lastSnapshotIncludeIndex = m_lastSnapshotIncludeIndex;
boostPersistRaftNode.m_lastSnapshotIncludeTerm = m_lastSnapshotIncludeTerm;
```

保存 Raft 安全性必须依赖的状态：

- `currentTerm`：防止旧 term 消息重新生效。
- `votedFor`：防止同一 term 重启后重复投票。
- `lastSnapshotIncludeIndex / Term`：日志被快照截断后，仍能进行日志匹配。

```cpp
for (auto& item : m_logs) {
  boostPersistRaftNode.m_logs.push_back(item.SerializeAsString());
}
```

`m_logs` 是 Protobuf 生成的 `LogEntry` 对象。这里先让 Protobuf 把单条日志序列化成 string，再交给 Boost 保存 `vector<string>`。

```cpp
std::stringstream ss;
boost::archive::text_oarchive oa(ss);
oa << boostPersistRaftNode;
return ss.str();
```

使用 Boost.Serialization 把整个 `BoostPersistRaftNode` 写入字符串流，并返回字符串。

---

##### 2) `Raft::readPersist()`：从字符串恢复 Raft 状态（1065-1085）

```cpp
void Raft::readPersist(std::string data) {
```

`data` 是 `Persister::ReadRaftState()` 从文件读出的 raft state。

```cpp
if (data.empty()) {
  return;
}
```

没有持久化数据时直接返回。通常表示第一次启动。

```cpp
std::stringstream iss(data);
boost::archive::text_iarchive ia(iss);
```

把字符串包装成输入流，并创建 Boost 输入归档器。

```cpp
BoostPersistRaftNode boostPersistRaftNode;
ia >> boostPersistRaftNode;
```

反序列化出临时对象。

```cpp
m_currentTerm = boostPersistRaftNode.m_currentTerm;
m_votedFor = boostPersistRaftNode.m_votedFor;
m_lastSnapshotIncludeIndex = boostPersistRaftNode.m_lastSnapshotIncludeIndex;
m_lastSnapshotIncludeTerm = boostPersistRaftNode.m_lastSnapshotIncludeTerm;
```

恢复 Raft 元状态。

```cpp
m_logs.clear();
for (auto& item : boostPersistRaftNode.m_logs) {
  raftRpcProctoc::LogEntry logEntry;
  logEntry.ParseFromString(item);
  m_logs.emplace_back(logEntry);
}
```

清空当前日志，再把保存的日志字符串逐条用 Protobuf 反序列化回 `LogEntry`。

---

##### 3) `Raft::persist()`：普通 Raft 状态落盘入口（603-609）

```cpp
void Raft::persist() {
  auto data = persistData();
  m_persister->SaveRaftState(data);
}
```

普通持久化只保存 raft state，不保存 KV snapshot。

调用链：

```text
Raft 状态变化
  -> persist()
  -> persistData()
  -> Persister::SaveRaftState()
```

典型触发场景：

- `currentTerm` 变化。
- `votedFor` 变化。
- `m_logs` 追加或被修改。

---

##### 4) `Raft::init()`：启动时恢复持久化状态（988-1047）

```cpp
m_peers = peers;
m_persister = persister;
m_me = me;
```

保存外部传入的 RPC peers、持久化器和当前节点 id。

为什么 `Persister` 从外部传进来？因为它同时被 `KvServer` 和 `Raft` 使用：

```text
Raft 用它保存/恢复 raft state
KvServer 用它读取 snapshot 恢复状态机
```

所以由 `KvServer` 创建，再传给 `Raft::init()`。

```cpp
m_mtx.lock();
```

初始化和恢复期间加锁，避免半初始化状态被并发访问。

```cpp
m_currentTerm = 0;
m_status = Follower;
m_commitIndex = 0;
m_lastApplied = 0;
m_logs.clear();
```

设置默认初始状态。这些值后面可能被持久化数据覆盖。

```cpp
for (int i = 0; i < m_peers.size(); i++) {
  m_matchIndex.push_back(0);
  m_nextIndex.push_back(0);
}
```

初始化 Leader 才会用到的两个数组。此时只是占位，真正成为 Leader 时会重新设置。

```cpp
m_votedFor = -1;
m_lastSnapshotIncludeIndex = 0;
m_lastSnapshotIncludeTerm = 0;
m_lastResetElectionTime = now();
m_lastResetHearBeatTime = now();
```

设置投票、快照边界和定时器初值。

```cpp
readPersist(m_persister->ReadRaftState());
```

核心恢复点：

```text
Persister 从文件读取 raft state 字符串
  -> readPersist 反序列化
  -> 覆盖 currentTerm / votedFor / logs / snapshot边界
```

```cpp
if (m_lastSnapshotIncludeIndex > 0) {
  m_lastApplied = m_lastSnapshotIncludeIndex;
}
```

如果存在快照，说明快照点及之前的日志已经体现在状态机里。恢复后 `lastApplied` 至少要跳到快照点，避免快照之前的日志再次 apply。

这里源码有 TODO：

```cpp
// rf.commitIndex = rf.lastSnapshotIncludeIndex
```

语义上更完整的恢复通常是：

```cpp
m_commitIndex = std::max(m_commitIndex, m_lastSnapshotIncludeIndex);
m_lastApplied = std::max(m_lastApplied, m_lastSnapshotIncludeIndex);
```

因为快照点之前一定已经 committed 并 applied。当前实现不恢复 `commitIndex` 一般不会马上破坏安全性，因为后续 Leader 的 AE 会同步 `leaderCommit`，但会出现 `lastApplied > commitIndex` 的不优雅状态。

```cpp
m_mtx.unlock();
```

恢复完成后释放锁。

后面创建 `IOManager` 并启动 `leaderHearBeatTicker / electionTimeOutTicker / applierTicker`，不是持久化本身，但它们会继续驱动后续日志复制、提交和 apply。

---

##### 5) `Raft::Snapshot()`：本节点主动做快照（1087-1122）

```cpp
void Raft::Snapshot(int index, std::string snapshot) {
```

`index` 表示快照覆盖到哪条 Raft 日志。`snapshot` 是上层 `KvServer` 做好的状态机快照字符串。

这里的 `snapshot` 内容不是 Raft 生成的。Raft 只把它当 bytes/string 保存；真正内容由 `KvServer` 决定。

```cpp
std::lock_guard<std::mutex> lg(m_mtx);
```

快照会修改日志数组和快照边界，必须加锁。

```cpp
if (m_lastSnapshotIncludeIndex >= index || index > m_commitIndex) {
  return;
}
```

拒绝非法快照：

- `index <= 已有快照点`：旧快照，无意义。
- `index > commitIndex`：还没提交的日志不能被做进快照。

```cpp
auto lastLogIndex = getLastLogIndex();
```

记录压缩前最后一条日志 index，后面用于断言。

```cpp
int newLastSnapshotIncludeIndex = index;
int newLastSnapshotIncludeTerm = m_logs[getSlicesIndexFromLogIndex(index)].logterm();
```

确定新的快照边界：快照覆盖到 `index`，边界 term 是该日志的 term。

```cpp
std::vector<raftRpcProctoc::LogEntry> trunckedLogs;
for (int i = index + 1; i <= getLastLogIndex(); i++) {
  trunckedLogs.push_back(m_logs[getSlicesIndexFromLogIndex(i)]);
}
```

只保留快照点之后的日志。`index` 以及之前的日志已经被状态机快照覆盖，可以删除。

```cpp
m_lastSnapshotIncludeIndex = newLastSnapshotIncludeIndex;
m_lastSnapshotIncludeTerm = newLastSnapshotIncludeTerm;
m_logs = trunckedLogs;
```

更新快照边界并替换日志数组。

```cpp
m_commitIndex = std::max(m_commitIndex, index);
m_lastApplied = std::max(m_lastApplied, index);
```

在当前函数里，前面的判断已经保证 `index <= m_commitIndex`，所以第一行大多是冗余保护；第二行表达“快照点之前已经应用”的语义。

```cpp
m_persister->Save(persistData(), snapshot);
```

这是快照落盘核心：

```text
persistData() 保存新的 Raft 元状态和剩余日志
snapshot 保存 KVServer 的状态机数据
Persister::Save() 同时写 raftstate + snapshot
```

```cpp
myAssert(m_logs.size() + m_lastSnapshotIncludeIndex == lastLogIndex, ...);
```

检查日志压缩前后的边界是否一致。

---

##### 6) `Raft::InstallSnapshot()`：Follower 接收 Leader 快照（420-475）

```cpp
m_mtx.lock();
DEFER { m_mtx.unlock(); };
```

手动加锁，并保证函数退出时自动解锁。

```cpp
if (args->term() < m_currentTerm) {
  reply->set_term(m_currentTerm);
  return;
}
```

旧 term 的 Leader 发来的快照直接拒绝。

```cpp
if (args->term() > m_currentTerm) {
  m_currentTerm = args->term();
  m_votedFor = -1;
  m_status = Follower;
  persist();
}
```

发现更高 term，当前节点退回 Follower，并持久化新的 term/vote。

```cpp
m_status = Follower;
m_lastResetElectionTime = now();
```

合法 Leader 的快照 RPC 会让当前节点确认自己是 Follower，并重置选举定时器。

```cpp
if (args->lastsnapshotincludeindex() <= m_lastSnapshotIncludeIndex) {
  return;
}
```

旧快照不安装。

```cpp
auto lastLogIndex = getLastLogIndex();
```

记录本地最后日志 index。

```cpp
if (lastLogIndex > args->lastsnapshotincludeindex()) {
  m_logs.erase(m_logs.begin(), m_logs.begin() + getSlicesIndexFromLogIndex(args->lastsnapshotincludeindex()) + 1);
} else {
  m_logs.clear();
}
```

如果本地日志超过快照点，删除快照点及之前的日志；否则全部清空。

为什么能截断？因为 Leader 发来的 snapshot 表示：`lastsnapshotincludeindex` 及之前的状态已经被压缩进 snapshot。安装后这些日志不再需要。

```cpp
m_commitIndex = std::max(m_commitIndex, args->lastsnapshotincludeindex());
m_lastApplied = std::max(m_lastApplied, args->lastsnapshotincludeindex());
m_lastSnapshotIncludeIndex = args->lastsnapshotincludeindex();
m_lastSnapshotIncludeTerm = args->lastsnapshotincludeterm();
```

推进 commit/apply 到快照点，并更新快照边界。

```cpp
ApplyMsg msg;
msg.SnapshotValid = true;
msg.Snapshot = args->data();
msg.SnapshotTerm = args->lastsnapshotincludeterm();
msg.SnapshotIndex = args->lastsnapshotincludeindex();
```

构造 snapshot 类型的 `ApplyMsg`，交给上层 KVServer 安装状态机快照。

```cpp
std::thread t(&Raft::pushMsgToKvServer, this, msg);
t.detach();
```

异步把快照消息推给 KVServer。

```cpp
m_persister->Save(persistData(), args->data());
```

把新的 Raft 状态和收到的状态机快照一起落盘。

---

##### 7) `Raft::leaderSendSnapShot()`：Leader 给落后节点发快照（528-560）

```cpp
m_mtx.lock();
raftRpcProctoc::InstallSnapshotRequest args;
```

加锁读取当前 Leader 的快照元信息，构造 RPC 请求。

```cpp
args.set_leaderid(m_me);
args.set_term(m_currentTerm);
args.set_lastsnapshotincludeindex(m_lastSnapshotIncludeIndex);
args.set_lastsnapshotincludeterm(m_lastSnapshotIncludeTerm);
args.set_data(m_persister->ReadSnapshot());
```

把 Leader id、term、快照边界和 snapshot 内容放入请求。snapshot 内容来自 `Persister::ReadSnapshot()`。

```cpp
raftRpcProctoc::InstallSnapshotResponse reply;
m_mtx.unlock();
bool ok = m_peers[server]->InstallSnapshot(&args, &reply);
```

RPC 调用前释放锁。参数传地址是因为工具函数签名接收指针：`args` 是输入参数，`reply` 是输出参数。

```cpp
m_mtx.lock();
DEFER { m_mtx.unlock(); };
```

RPC 返回后重新加锁处理回复。

```cpp
if (!ok) {
  return;
}
if (m_status != Leader || m_currentTerm != args.term()) {
  return;
}
```

网络失败、身份过期、term 过期的回复都丢弃。

```cpp
if (reply.term() > m_currentTerm) {
  m_currentTerm = reply.term();
  m_votedFor = -1;
  m_status = Follower;
  persist();
  m_lastResetElectionTime = now();
  return;
}
```

如果对方 term 更新，Leader 退回 Follower 并持久化。

```cpp
m_matchIndex[server] = args.lastsnapshotincludeindex();
m_nextIndex[server] = m_matchIndex[server] + 1;
```

认为该 follower 至少同步到了快照点。更防御性的写法应该使用 `max`，避免 RPC 乱序返回导致 Leader 低估 follower 进度。

---

##### 8) `Raft::GetRaftStateSize()`：查询 raft state 大小（746）

```cpp
int Raft::GetRaftStateSize() { return m_persister->RaftStateSize(); }
```

只是转发给 `Persister`。它给 `KvServer` 判断是否需要做快照：

```text
KvServer apply 日志后
  -> GetRaftStateSize()
  -> 过大则 MakeSnapShot()
  -> Raft::Snapshot()
```

---

#### 相关触发点

这些函数本身不一定是“持久化函数”，但会触发 `persist()`：

| 函数 | 行号 | 触发原因 |
| --- | --- | --- |
| `AppendEntries1()` | 8 ~ 24 | 合法 AE 可能修改 term/vote/logs，使用 `DEFER { persist(); }` |
| `doElection()` | 202 ~ 218 | 自增 term、给自己投票 |
| `RequestVote()` | 611 ~ 618 | 投票逻辑结束后持久化 |
| `sendRequestVote()` | 761 ~ 823 | 发现更高 term 或当选 Leader |
| `Start()` | 947 ~ 972 | Leader 追加新日志 |

记忆规则：

```text
term 变了 -> persist
votedFor 变了 -> persist
logs 变了 -> persist
snapshot 边界变了 -> Save(persistData(), snapshot)
```

---

#### Q&A 集

**Q1：为什么 `m_persister = persister;`，而不是 Raft 自己 new 一个？**

因为 `Persister` 同时服务两层：

```text
Raft：保存/恢复 raft state
KvServer：读取/安装 snapshot，恢复 KV 状态机
```

所以由外层 `KvServer` 创建一个共享的 `Persister`，再传给 `Raft::init()`。

**Q2：`lastApplied` 为什么恢复到快照点？它崩溃前可能大于快照点啊。**

崩溃后，系统只能确定“快照点之前已经在状态机里”。快照点之后的 apply 进度没有被持久化，状态机也通常从 snapshot 恢复，所以要从快照点之后重新由 Raft commit/apply 驱动。

Raft 论文里 `lastApplied` 是 volatile state，不要求持久化。关键是 snapshot 覆盖范围不能重复 apply。

**Q3：那 `commitIndex` 到底要不要恢复到快照点？**

建议恢复：

```cpp
m_commitIndex = std::max(m_commitIndex, m_lastSnapshotIncludeIndex);
m_lastApplied = std::max(m_lastApplied, m_lastSnapshotIncludeIndex);
```

当前代码不恢复 `commitIndex` 一般不破坏安全性，因为 Leader 的 AppendEntries 会重新推进 `leaderCommit`。但语义上会出现 `lastApplied > commitIndex`，不够优雅。

**Q4：`InstallSnapshot(&args, &reply)` 为什么都是地址？**

RPC 工具函数签名接收指针。`args` 是请求输入，`reply` 是响应输出，RPC 调用会把结果写进 `reply`。

**Q5：`m_matchIndex[server] = args.lastsnapshotincludeindex()` 为什么最好用 `max`？**

`matchIndex` 表示 Leader 已知 follower 至少复制到哪里。这个值在同一任期 Leader 内应该单调递增。

直接赋值在多数情况下不会破坏安全性，但 RPC 乱序返回时可能低估 follower 进度，导致重复发日志/快照或影响后续 commit 推进速度。

更稳妥：

```cpp
m_matchIndex[server] = std::max(m_matchIndex[server],
                                args.lastsnapshotincludeindex());
m_nextIndex[server] = std::max(m_nextIndex[server],
                               m_matchIndex[server] + 1);
```

**Q6：已经 committed 的 index 是否必须小于等于所有节点的 `matchIndex`？**

不是。Raft 只要求多数派复制成功：

```text
commitIndex <= 多数派节点的 matchIndex
```

不是：

```text
commitIndex <= 所有节点的 matchIndex
```

落后 follower 的 `matchIndex` 小于 `commitIndex` 是合法状态，后续会慢慢追上。

**Q7：为什么 KVServer 有快照，Raft 也有快照？**

它们是同一个 snapshot 的两个视角：

| 层 | 负责 |
| --- | --- |
| KVServer | 生成/解析 snapshot 的内容：SkipList 数据、客户端去重表 |
| Raft | 管理 snapshot 对应的日志边界：index、term、日志截断、InstallSnapshot RPC |
| Persister | 把 raft state 和 snapshot 字符串一起落盘 |

**Q8：`snapshot` 是 KVServer 做好的状态机快照字符串，是什么意思？**

状态机就是 KV 数据库。`KvServer::MakeSnapShot()` 会把 SkipList 数据和去重表序列化成字符串。Raft 不理解这个字符串内容，只负责保存、转发和按 index/term 管理。

**Q9：`Snapshot()` 里 `m_commitIndex = std::max(m_commitIndex, index)` 有必要吗？**

当前函数前面已经保证 `index <= m_commitIndex`，所以这行大多是冗余保护。它表达的是“快照点之前一定已经 committed”的语义。

**Q10：`InstallSnapshot()` 为什么能直接截断日志？**

Leader 发来的 snapshot 表示：快照点及之前的状态已经被压缩进 snapshot。安装后这些旧日志不再需要。

如果本地日志比快照点更长，就保留快照点之后的日志；如果本地日志还没到快照点，就全部清空。之后仍靠 AppendEntries 的日志匹配机制修正快照点之后的分歧。

**Q11：apply 一条日志是什么意思？**

commit 表示日志已经被多数派确认，可以执行。apply 表示真正把这条命令交给状态机执行。

例如日志是：

```text
Put("x", "1")
```

apply 后才真正变成：

```text
SkipList["x"] = "1"
```

本项目链路：

```text
Raft::applierTicker()
  -> applyChan
  -> KvServer::ReadRaftApplyCommandLoop()
  -> KvServer::GetCommandFromRaft()
  -> ExecutePutOpOnKVDB / ExecuteAppendOpOnKVDB / ExecuteGetOpOnKVDB
```

**Q12：`MakeSnapShot()` 在哪里？**

在 `src/raftCore/kvServer.cpp`：

```cpp
std::string KvServer::MakeSnapShot()
```

它由 `KvServer::IfNeedToSendSnapShotCommand()` 调用，生成 snapshot 后再调用 `Raft::Snapshot(raftIndex, snapshot)`。

---

#### 可以优化的地方

1. **`Raft::init()` 恢复时同步设置 `commitIndex`。**

当前只设置 `lastApplied = lastSnapshotIncludeIndex`。建议同时：

```cpp
m_commitIndex = std::max(m_commitIndex, m_lastSnapshotIncludeIndex);
```

2. **`leaderSendSnapShot()` 更新 `matchIndex/nextIndex` 使用 `max`。**

避免 RPC 乱序返回导致 Leader 低估 follower 进度。

3. **`Snapshot()` 中截断日志可以更直接。**

当前用循环复制 `index+1` 到末尾，可以用 vector erase/slice 风格简化，同时注意边界。

4. **`Snapshot()` 中 `m_commitIndex = max(m_commitIndex, index)` 可标注为语义保护。**

当前前置条件已经保证它不改变值，容易让读者困惑。

5. **`InstallSnapshot()` 保留快照点之后日志时，应更明确检查快照边界日志 term。**

严格实现通常会判断本地 `snapshotIndex` 位置日志 term 是否等于 snapshotTerm；匹配才保留后续日志，否则清空后续日志，避免保留错误分支。

6. **持久化读写链路需要和 `Persister` 优化配套。**

`Persister::ReadRaftState()` / `ReadSnapshot()` 当前用 `>>` 读取字符串，可能截断 Boost 序列化结果。这个问题会直接影响 `readPersist()`。

7. **明确区分 Raft snapshot 元信息和 KV snapshot 内容。**

建议在注释中反复使用两个词：

```text
snapshot metadata: lastSnapshotIncludeIndex / Term
snapshot data: KvServer 序列化后的状态机数据
```

---

#### 一句话总结

> **`raft.cpp` 的持久化分两条线：普通 persist 保存 Raft 自己的 term/vote/log/snapshot 边界；snapshot 保存 KVServer 生成的状态机数据，并同步更新 Raft 的日志边界。Raft 不理解 KV snapshot 的内容，只负责按日志 index/term 管理它。**

#### 疑问 / TODO

- [ ] 修改 `Raft::init()`，恢复时设置 `commitIndex = max(commitIndex, lastSnapshotIncludeIndex)`，跑示例验证。
- [ ] 把 `leaderSendSnapShot()` 的 `matchIndex/nextIndex` 更新改成 `max`，验证 RPC 乱序下不会倒退。
- [ ] 检查 `InstallSnapshot()` 是否应在保留后续日志前比较 `lastsnapshotincludeterm`。
- [ ] 梳理 `KvServer::MakeSnapShot()` 和 `ReadSnapShotToInstall()` 的具体序列化内容，补一节 KVServer snapshot 日志。
- [ ] 写一个 Mermaid 图，把 `commit -> apply -> snapshot -> truncate` 的状态流单独画出来。

