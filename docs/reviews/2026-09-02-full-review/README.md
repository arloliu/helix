# Helix 全新觀點 Review Report

- 日期：2026-09-02
- 對象：`github.com/arloliu/helix` @ `7e93857`（v1.6.0 之後的 main）
- 方法：五個獨立 research agent 分別針對「架構、效能、fallback/failover、極端情境自動復原、replay/mirror 資料一致性」從原始碼重新推導（刻意不參考 `tmp/` 舊 review），並以 throwaway test / benchmark 驗證關鍵假設。主報告由我交叉比對、去重、再抽查驗證後整合。
- 標記：**CONFIRMED** = 已讀完整程式路徑或以測試/benchmark 重現；**SUSPECTED** = 由程式推論但未執行。
- 分項報告（英文，含完整 file:line 證據與重現步驟）：[`parts/01-architecture.md`](parts/01-architecture.md)、[`parts/02-performance.md`](parts/02-performance.md)、[`parts/03-fallback-failover.md`](parts/03-fallback-failover.md)、[`parts/04-auto-recovery.md`](parts/04-auto-recovery.md)、[`parts/05-replay-mirror.md`](parts/05-replay-mirror.md)。
- 設計決策紀錄：[`design-consensus/consensus.md`](design-consensus/consensus.md)（兩位獨立 reviewer 兩輪往返後的共識，已由維護者確認並寫入 [`docs/plans/roadmap.md`](../../plans/roadmap.md)）。

---

## 0. 執行摘要

Helix 對「秒級抖動」的處理相當成熟：hot path 幾乎無鎖、狀態轉換有 sequence 保護、事件與 drop 都有精確計數、panic 全面隔離、timestamp 紀律正確。但用「長時間故障」與「fleet 規模」的眼光看，有三個根本性缺口，其餘問題多是它們的衍生：

1. **Replay 不是 outage backlog，而是秒級 retry buffer。** MemoryWorker 預設 5 次嘗試、總等待 1.5 秒；NATS worker 失敗立刻 `Nak()` 無延遲、consumer 無 `BackOff`，`MaxDeliver=5` 在數秒內耗盡後 `Term()`。目標叢集只要掛超過幾秒，所有 replay 都被丟棄；叢集回來時 backlog 是空的，兩邊永久分歧。`docs/auto-recovery.md` 描述的「叢集回來後 worker 排空 backlog」在預設設定下不會發生。（CONFIRMED，已重現）
2. **五個獨立的 per-cluster 健康狀態機互不相通。** CircuitBreaker（只看 read）、AdaptiveDualWrite degraded（只看 write）、`clusterStats`（只餵 auto-refresh）、drain flag、read strategy 狀態，各自有一套錯誤排除表。結果是：LatencyCircuitBreaker open 了但 read 不改道；AdaptiveDualWrite degraded 後 auto-refresh 被餓死（永遠不 refresh 死掉的 session）；recovery probe 在 10 秒內默默撤銷操作員的 `ForceDegrade`。
3. **Caller 的 context 取消/逾時被當成叢集故障。** `isCtxErr` 已存在但只接在 slice read 的 fallback leg。其餘 read/write 路徑把 `context.Canceled`/`DeadlineExceeded` 記進 breaker、sticky 切換、auto-refresh 計數與 adaptive strike。一個短 deadline 的請求會同時在 A、B 各記一次失敗；三個就能讓兩個叢集都 degraded。

效能面：Helix 自身每筆操作的 overhead 小於 1 µs，相對 1–5 ms 的 CQL round trip 可忽略；真正的效能問題是結構性的：replay 排空是單執行緒（吞吐 = 1/latency），以及 NATS enqueue 是 write path 上的同步 publish（degraded 時每筆 write 都付一次 JetStream RTT，上限 5 秒且 caller 無法取消）。

---

## 1. 整體架構分析

### 1.1 元件圖

```
                          +--------------------+
                          |  types (leaf)      |  ClusterID, sentinel errors, ReplayPayload,
                          |  只 import stdlib  |  MetricsCollector, Logger, ClusterEvent,
                          +---------^----------+  7 個 optional metrics 擴充介面
                                    |
     +------------+------------+----+----+-------------+-------------+
     |            |            |         |             |             |
+----+-----+ +----+----+ +-----+----+ +--+---------+ +-+----------+ +--+---------+
| adapter/ | | policy  | | replay   | | internal/  | | contrib/   | | mirror     |
| cql      | | read/   | | Memory,  | | logging    | | metrics/vm | | Engine     |
| v1 / v2  | | write/  | | NATS,    | | metrics    | |            | | (import    |
|          | | failover| | Worker   | | typeutil   | |            | |  replay)   |
+----+-----+ +---------+ +----^-----+ +-----^------+ +------------+ +-----^------+
     |         (policy 不 import root；    |                            |
     |          以 structural typing 滿足) |                            |
     +-----------+------------+------------+----------------------------+
                 |
        +--------v-----------------------------------------------------------+
        |  helix (root)                                                      |
        |  strategy.go    : ReadStrategy / WriteStrategy / FailoverPolicy /  |
        |                   Replayer / ReplayWorker / TopologyWatcher /      |
        |                   StrictWriter / LatencyRecorder                   |
        |  config.go      : ClientConfig + 30 個 WithXxx                     |
        |  cql_client.go  : CQLClient、read/write 編排、auto-refresh、        |
        |                   recovery probe、topology watcher、query/batch/iter|
        |  events.go      : eventDispatcher                                  |
        |  mirror_dispatch.go                                                |
        +--------^-----------------------------------------------------------+
                 |
        +--------+---------+
        | topology         |   import ROOT（TopologyUpdate / TopologyWatcher）
        | Local, NatsKV    |   → root 永遠不能 import topology
        +------------------+
```

### 1.2 Write 生命週期（`Query().Exec()`）

```
ExecContext
 ├─ 單叢集 fast path → sessionA 直接執行 → recordOpOutcome
 └─ executeWriteWithReplay
      ├─ 兩邊 drain → ErrBothClustersDraining
      ├─ 一邊 drain → executeWriteWithDrain（繞過 WriteStrategy，健康邊直寫，drain 邊 enqueue replay）
      └─ executeDualWrite
           ├─ Strict → executeStrictDualWrite（PartialWriteError / DualClusterError，不 replay）
           ├─ WriteStrategy.Execute(ctx, writeA, writeB)
           │    Concurrent：並行等兩邊；Sync：序列；Adaptive：degraded 邊 fire-and-forget → ErrWriteAsync/Dropped
           ├─ 分類：nil | ErrWriteAsync | ErrWriteDropped | real error
           ├─ 兩邊 real error → DualClusterError（不 replay）
           └─ 任一邊非 nil → enqueueReplayIfNeeded（WithoutCancel）→ 回傳 nil
```

### 1.3 Read 生命週期（`Query().Scan()` / `Iter()`）

```
executeRead
 ├─ resolveReadTarget：AllowedClusters override（fail-closed）或 ReadStrategy.Select
 ├─ runPrimaryRead：drain-aware 改選（僅非 override、非 paged）→ readFunc
 ├─ nil → OnSuccess + RecordSuccess（若實作 LatencyRecorder 則只呼叫 RecordLatency）
 ├─ ErrNotFound / ErrRowLimitExceeded → 非健康訊號；FallbackRead 開啟時試另一邊一次
 └─ 其他錯誤（含 ctx 錯誤）→ IncReadError + recordOpOutcome + RecordFailure
      → ShouldFailover? 否：回傳錯誤；是：OnFailure → 另一邊（同一個 ctx）→ 成功或 DualClusterError

Iter()：resolveReadTarget 後直接建 iterator；Close() 只記 OnSuccess/recordOpOutcome，
        不 failover、不餵 breaker、不檢查 drain。
CAS：ReadStrategy.Select 直選，忽略 override 與 drain。
```

### 1.4 背景 goroutine 與 Close 順序

| Goroutine | Close 時 | 有 join? |
|---|---|---|
| watchTopology | ctx cancel | 否 |
| replay Worker | Stop() | 是 |
| mirror Engine workers (4) + mirror replay worker | Stop() | 是 |
| eventDispatcher | stop() | 是 |
| autoRefreshLoop | ctx cancel | **否** |
| recoveryProbeLoop ×2 | cancel + WaitGroup | 是 |
| AdaptiveDualWrite fire-and-forget (≤100) | 不處理 | 否（文件有說明） |

Close 順序：topology → auto-refresh → mirror → replay worker → probe → events → session A/B。順序正確，但 auto-refresh 未 join：`RefreshSession` 與 `Close` 競爭時可能漏掉一個新建的 session（`parts/01` F-08）。

### 1.5 優點（CONFIRMED）

- `types` 真的是 leaf；driver 完全隔離在 adapter 後面，root 不 import 任何 gocql。
- Hot path 無鎖：session 用 `atomic.Pointer` 交換、`clusterStats` 全 atomic、read strategy `Select` 0 alloc。
- CircuitBreaker / AdaptiveDualWrite 的狀態轉換用 per-cluster mutex + transition sequence，避免過時 gauge 覆寫；事件在鎖外送出。
- 所有外部 callback（write leg、probe、override fn、event handler）都有 panic 隔離。
- Sentinel error 分類一致：`ErrWriteAsync/Dropped/NotFound/RowLimitExceeded/ClusterDegraded/ClusterDraining` 在 root 與 policy 都不算健康訊號。
- AllowedClusters override fail-closed，並有 log 風暴保護。
- 測試金字塔真實存在：root 339 / policy 150 / replay 101 個單元測試、134 整合、52 e2e、14 個 simulation scenario + chaos session + write tracker。

### 1.6 結構性問題

| ID | 嚴重度 | 問題 | 證據 |
|---|---|---|---|
| A-1 | High | **五個獨立健康狀態機**（見摘要 #2）。write 從不呼叫 `FailoverPolicy.RecordFailure`；read 從不看 `IsDegraded`；probe 只治 write strategy；refresh 只重設 `clusterStats`。 | `cql_client.go:2437,2454,2479,2563`（RecordFailure 全在 read path）、`:355-412`（probe） |
| A-2 | High | **`cql_client.go` 3,858 行 god file**，八種職責糾纏：lifecycle、auto-refresh、probe、DI wiring、topology、read router、三個 write orchestrator、query/batch/iter 實作。read pipeline 有六個入口，各自一套錯誤分類表，正是 ctx 錯誤只修一處的原因。 | 見 `parts/01` F-03 的行號分佈 |
| A-3 | Medium | **擴充機制靠 optional interface duck typing，一半未匯出**：`probeReporter`、`eventAware`、`metricsAware`、`loggerAware` 都是 unexported；自訂 WriteStrategy 作者無從得知實作 `IsDegraded+RecordProbeSuccess` 才會啟用 probe。`policy/` 沒有 `var _ helix.WriteStrategy = ...` 編譯期斷言。 | `cql_client.go:342,595,529-535`；`mirror_dispatch.go:236-243` 對具體型別 switch |
| A-4 | Medium | **`ClientConfig` 同時是使用者選項、runtime registry、與 `Config()` 回傳的可變指標**；hot path 無同步讀取 `c.config.*`，post-construction 修改即 data race。simulation 程式碼就在用 `client.Config().ReplayWorker = worker`。 | `config.go:84-247`、`cql_client.go:3856`、`test/simulation/simulation.go:348` |
| A-5 | Medium | **Drain 寫入繞過 WriteStrategy**：`executeWriteWithDrain` 直寫健康邊，AdaptiveDualWrite 看不到 latency 樣本，drain 邊沒有 `IncWriteSkipped`；strict 語意在此 inline 重複三次。 | `cql_client.go:1795-1882` |
| A-6 | Medium | **`Iter()`、batch `IterContext()`、CAS 忽略 drain**，文件卻說 reads 會避開 draining cluster。無任何測試把 `Iter` 與 `SetDrain` 配對。 | `cql_client.go:3204-3224, 1501-1506` |
| A-7 | Medium | **`WithAutoMemoryWorker` 無條件覆寫使用者的 `Replayer`/`ReplayWorker`**，`root_validation.go` 未攔截；`WithMirrorReplayer` 無 `WithMirror`、`WithRecoveryProbe` 配非 adaptive strategy 也都是靜默無效。 | `cql_client.go:844-862` |
| A-8 | Medium | **`LatencyRecorder` 靜默取代 `RecordSuccess`**：實作 LatencyRecorder 的 policy 永遠收不到 `RecordSuccess`；內建 LCB 內部自己補呼叫所以沒事，自訂 policy 會壞。 | `cql_client.go:2206-2218` |
| A-9 | Low | `New*` 與 `New*Checked` 雙建構子：前者靜默 normalize 非法值、後者回傳 error；README 推薦前者。 | `policy/failover_policy.go:386-397` 等 |
| A-10 | Low | 文件漂移：`DefaultConfig` 說 cluster name 預設 `"ClusterA"`，實際 `"A"`；`mirror/` godoc 引用不存在的 `types.Replayer`；`100-overview.md` 說共用介面都在 `types`，實際 strategy 介面在 root；`topology` import root 而非 `types` 沒有說明理由。 | `config.go:270`、`mirror/engine.go:21,34` |
| A-11 | Low | 設定面：root 30 + policy 25 + replay 31 + mirror 7 + topology 4 = 97 個 option；`NowProvider` 是唯一沒有 `With*` 的公開欄位。 | `config.go` |

**架構改善方向（尊重「最小變更」原則）**

- Tier 1（純檔案拆分，零行為/API 變更，一個 PR）：`session_lifecycle.go`、`recovery_probe.go`、`wiring.go`、`read_path.go`、`write_path.go`、`slice_read.go`、`query.go`、`batch.go`、`iter.go`。不拆 sub-package（`topology` 已 import root，拆了會動公開 import path）。
- Tier 2（內部型別，仍無公開 API 變更）：
  - `readRouter` 擁有 `resolveReadTarget`、drain 改選與 failover 分支，並提供**唯一的** `classifyReadErr(err) → {ok, notFound, rowLimit, ctxErr, clusterErr}`，六個 read 入口都走它。A-1/摘要 #3 就不可能再回歸。
  - Drain 改成 per-leg sentinel `ErrClusterDraining` 從 `writeA/writeB` closure 回傳，讓 `executeDualWrite` 用與 `ErrClusterDegraded` 相同的 switch 處理，刪掉 `executeWriteWithDrain`（A-5）。
  - `ClientConfig` 拆成使用者選項 struct 與 unexported runtime struct，`Config()` 回傳 copy（A-4）。
- Tier 3（跨狀態機）：root 內部 `clusterHealth` hub 擁有 `clusterStats` 與 drain flag，提供 `observe(cluster, op, err, latency)`；所有 `recordOpOutcome`/`RecordFailure`/`RecordLatency` 經過它。先只做觀測匯流，之後才考慮 opt-in 的訊號轉發（breaker open → ForceDegrade、probe success → RecordSuccess）。
- 匯出 `ProbeReporter`、`EventEmitterSetter`、`Instrumentable` 三個能力介面，並在 `policy/` 加編譯期斷言（A-3）。
- `root_validation.go` 加三條五行的組合檢查（A-7）。

---

## 2. 效能分析

### 2.1 測量基線（mock session，AMD 9950X3D，go1.26）

| 路徑 | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| 直接 mock Exec（地板） | 8 | 0 | 0 |
| 單叢集 `Exec` | 155 | 208 | 3 |
| 雙叢集 `Exec`（nil strategy） | 698 | 464 | 12 |
| 雙叢集 `Exec` + Concurrent/Sticky/CB | 616 | 480 | 13 |
| 部分失敗 + replay enqueue | 826 | 636 | 17 |
| 雙叢集 `Scan` | 171 | 80 | 3 |
| 雙叢集 `Scan` 32 執行緒並行 + ActiveFailover | 26 | 40 | 3 |
| 雙叢集 `Scan` 32 執行緒並行 + CircuitBreaker | 145 | 40 | 3 |
| 雙叢集 `Exec` 並行，NopMetrics / vm collector | 204 / 447 | 384 | 12 |
| `AdaptiveDualWrite.Execute` 並行 vs `ConcurrentDualWrite` | 230 / 53 | 136 / 104 | 6 / 4 |
| `NATSReplayer.Enqueue`（loopback JetStream，同步） | 15,100 | 3,000 | 43 |
| raw `js.PublishAsync` 參考值 | 1,600 | 2,100 | 31 |
| Memory replay worker 排空 2000 筆 @1ms execute | 949 筆/秒 | | |
| `sync.WaitGroup.Go` 地板 | 378 | 64 | 3 |

結論：Helix 自身 overhead 相對真實 CQL round trip 小於 0.1%。ns/op 層級的優化價值有限，真正該投資的是下面兩個 High。

### 2.2 發現

| ID | 嚴重度 | 問題 | 方向 |
|---|---|---|---|
| P-1 | **High** | **Replay 排空單執行緒**：MemoryWorker 第一次嘗試 inline 在唯一的 dequeue goroutine；NATS worker 每叢集一個 goroutine 且 batch 內序列執行。吞吐 = 1/latency（實測 949/s @1ms；真實 2–5 ms 寫入 → 200–500/s）。5 分鐘 5k writes/s 的 outage 留下 150 萬筆，要 50–125 分鐘才排完。 | 兩個 backend 都加 bounded executor pool（`WithReplayConcurrency(n)`，預設 8–16）；per-cluster queue 讓慢的 A 不擋 B。順序性已由 LWW timestamp 保證，且現在 retry goroutine 本來就不保序。 |
| P-2 | **High** | **`NATSReplayer.Enqueue` 是 write path 上的同步 JetStream publish**，`PublishTimeout` 5 秒，caller ctx 被 `WithoutCancel` 包住無法縮短。AdaptiveDualWrite degraded 或 drain 時**每筆** write 都 enqueue，等於每筆 write 多付一次 JetStream RTT，NATS 慢時上限 5 秒。這與 fire-and-forget「不要 block 在慢的那邊」的初衷相反。 | `js.PublishAsync` + bounded pending + 背景 goroutine 收 ack future（失敗走 `IncReplayDropped`/event）；或前置小型 ring buffer + batching publisher。以 option 提供，若「Exec 回傳即 durable」是契約則預設保留同步。附帶：encode buffer 用 `sync.Pool`。 |
| P-3 | Medium | `CircuitBreaker.RecordSuccess` 每次成功 read 都拿 mutex：32 執行緒下 +120 ns/read。 | 開頭加 lock-free fast path `if failures.Load()==0 { return }`；不變式 `failures==0 ⇒ !tripped` 已驗證成立。 |
| P-4 | Medium | `AdaptiveDualWrite` 每筆 write 鎖兩個叢集的 mutex（`recordFast`）只為了把 `slowStrikes` 歸零，並多逃逸 2 個物件：並行下比 Concurrent 慢 4.4 倍。 | strike 計數改 atomic，`recordFast` 加 `!isDegraded && slowStrikes==0` fast path；per-call 狀態合併成一個 struct。 |
| P-5 | Medium | `contrib/metrics/vm` histogram 用 VictoriaMetrics 的 mutex-per-Update：並行 +243 ns/dual write（純 contention）。 | 每個 bucket 改 atomic counter、`_sum` 用 atomic int64 ns 經 gauge callback 暴露；可選 sharding。 |
| P-6 | Medium | `executeDualWrite` 每次 7 個 heap 物件（closure ×2、`errB`、`wg`、`wg.Go` closure…）；`executeStrictDualWrite` 重複同一形狀。Prototype：6→2 allocs，−12% serial / −16% parallel。 | 單一 `dualWriteJob` struct；**不要** `sync.Pool`（fire-and-forget 會在 Execute 回傳後持有 `writeB`）。 |
| P-7 | Medium | 每個 fluent setter（`Consistency`、`WithTimestamp`、`PageSize`、`MaxRows`…）都 `&c` 逃逸到 heap，一個 alloc。 | value field + `set` bitmask。 |
| P-8 | Medium | gocql v1 adapter 每次操作 alloc 兩個 296 B `Query`：Helix 從不 `Release()` 所以 gocql pool 永遠空；每個 `*Context` 方法又 `WithContext` copy 一次。v2 只 alloc 一次。 | `WithContext` 後對原 query 呼叫 `Release()`；或文件標明 v2 為效能首選。SUSPECTED 節省量需真 session 量測。 |
| P-9 | Low | 每次 read/write 讀三次時鐘；memory worker 用 `time.After(100ms)` 輪詢而非已存在的 blocking `Dequeue(ctx)`；NATS worker 每筆訊息兩個 timer；`Columns()` 複製兩次；batch entries 從 cap 0 成長；`gocql.UUID` 走 16 次 reflect（+15 allocs）；`Mirror()` 開啟時每次 capture 一個 RWMutex RLock。 | 各自五行內的修正，見 `parts/02` L1–L9。 |

### 2.3 已經做得好的

Session swap 無鎖；`resolveReadTarget` 無 override 時 0 alloc、7 ns；read strategy `Select` 0.7–4 ns；`Scan` closure 不逃逸（雙叢集 read 與單叢集同樣 3 allocs）；slice read 不 double-buffer；每筆 dual write 只開一個 goroutine；batch replay 轉換是 lazy 的；metrics 預設 Nop 且 vm collector 預建所有 series；mirror 未啟用時零成本；msgp 編解碼 12–70 ns；背景 goroutine 閒置成本極低；沒發現 `time.After` 洩漏。

---

## 3. Fallback 分析（read 端 FallbackRead 與 failover 執行）

### 3.1 行為摘要：A 故障後一筆 read 的流程

| 觸發 | 元件 | 動作 |
|---|---|---|
| read 回非 sentinel 錯誤 | `executeRead` | `IncReadError`、`recordOpOutcome(fail)`、`RecordFailure(A)` |
| `ShouldFailover` = false | `executeNormalFailover` | 錯誤直接回 caller，**不重試** |
| `ShouldFailover` = true | `ReadStrategy.OnFailure` | 選另一邊；StickyRead 在 cooldown 外會換 preferred |
| 另一邊 draining | `executeNormalFailover` | 拒絕 failover，回 A 的錯誤 |
| 另一邊成功 | `tryFallbackCluster` | `IncFailoverTotal`、`EventFailover`、`OnSuccess(B)`、`RecordSuccess(B)` |
| 另一邊失敗 | `tryFallbackCluster` | `RecordFailure(B)`、`DualClusterError` |
| `ErrNotFound` + FallbackRead | `executeFallbackRead` | 另一邊試一次；找到則 `IncReadDivergence` + event |

### 3.2 發現

| ID | 嚴重度 | 問題 | 失敗情境 | 方向 |
|---|---|---|---|---|
| FB-1 | **High** | **Caller ctx 取消/逾時被當叢集故障**（摘要 #3）。`isReadTerminalNonHealth` 只排除 NotFound/RowLimit；failover leg 用同一個已死的 ctx。 | T2：StickyRead + CB(1)，一個 caller 取消 → `DualClusterError{canceled, canceled}`、sticky 切到 B 五分鐘、A 與 B 各 +1 failure。T5：A 卡住、caller deadline 30 ms → failover 到 B 時 ctx 已過期，永遠失敗；RoundRobin 下整個 outage 期間 50% read 失敗。 | 所有 leg 對 `isCtxErr` 一律不記健康、不呼叫 `OnFailure/RecordFailure`；`ctx.Err()!=nil` 時跳過 failover；提供 `WithReadAttemptTimeout` 讓 primary attempt 有界。 |
| FB-2 | **High** | **`Iter`+`PageState` 會把 cursor 送到另一個叢集**：`IterContext` 沒設 `preserveSelectedCluster`，slice path 自己的註解已說明這「unsound」。 | 第一頁在 A，中間 sticky 因別的失敗切到 B，第二頁帶 A 的 cursor 打 B → driver error 或靜默跳/重複列（Cassandra 與 ScyllaDB paging state 格式不同）。 | `pageState != nil` 時 `preserveSelectedCluster: true`；更好是把 cluster ID 編進 Helix 回傳的 `PageState`。 |
| FB-3 | Medium | **`CircuitBreaker` 在前 threshold−1 次失敗、以及每個 `resetTimeout` 間隔後，都拒絕 failover**；half-open probe 是犧牲一筆真實使用者請求。 | T1：threshold 3，A 掛 → 前兩筆 read 直接回錯。T8：RoundRobin + threshold 2 / reset 10 ms，40 筆 read 有 4 筆對使用者可見失敗。read 端 breaker 沒有「避免送流量」的效果（見 FO-1），所以它相對 ActiveFailover 純粹增加使用者可見錯誤。 | 拆開「可否在另一邊重試這筆 read」（只要另一邊不 drain 就是可以）與「strategy 是否該換 preferred」（threshold 閘控）。half-open 探測改由背景 probe 做，不拿使用者請求當祭品。 |
| FB-4 | Medium | **`StickyRead` cooldown 把所有 read 釘在死掉的叢集上長達 5 分鐘**：cooldown 內每筆 read 先打 preferred（已死）再打另一邊。 | T10：A 抖一下 → preferred=B；B 隨即真掛 → 十筆 read 全部先付 B 的失敗 latency；配 CB 前 threshold−1 筆直接失敗。 | 當「現在的 preferred」本身失敗時允許 cooldown 內切換（這不是震盪，是兩邊接連失敗）；或改用 per-cluster 連續失敗計數取代 wall-clock cooldown；提供 `Reset()/SetPreferred()`。 |
| FB-5 | Medium | **Iterator 錯誤對 failover policy 與 read strategy 不可見**：`cqlIter.Close` 只呼叫 `recordOpOutcome`+`OnSuccess`。 | T7：五次 `Iter().Close()` 錯誤 + CB(1) → `Failures(A)==0`、sticky 仍在 A、B 從未被碰。Iter 為主的服務永遠不 failover；被 `Scan` 打開的 breaker 也不會被成功的 Iter 關閉。 | Close 錯誤（非 ctx）時呼叫 `RecordFailure` 與 `OnFailure`（忽略回傳，Iter 無法重試）；乾淨 Close 呼叫 `RecordSuccess`。 |
| FB-6 | Medium | **`Scan`/`MapScan` 的 FallbackRead 會從 draining cluster 讀**：`skipDrainingAlt` 只有 slice 方法會設。 | 健康邊 row 真的被刪 → 從正在 backfill 的 draining 邊讀到復活的舊 row。 | 所有 FallbackRead leg 預設跳過 draining，另提供 `FallbackReadIncludingDraining()`。 |
| FB-7 | Low | `WithTimestamp(0)` 或 provider 回 0 會被 gocql 視為「用現在」，replay 時拿到新 timestamp 覆寫較新的直寫。 | | validation 時拒絕 0；文件註明 fleet clock skew 影響 LWW。 |
| FB-8 | Low | CAS 路由忽略 override 與 drain，且被 read 端的抖動（FB-1）連帶切換：所有 LWT 靜默搬到另一個叢集。 | | `WithCASCluster` 或至少 drain-aware。 |
| FB-9 | Low | 兩邊都 drain 時 read 照常進行且可 failover 到另一個 draining，write 則 fail fast；`IsDraining` 文件沒說。 | | 補文件。 |

---

## 4. Failover 分析（policy 狀態機、write 端、session refresh、可觀測性）

| ID | 嚴重度 | 問題 | 證據 / 失敗情境 | 方向 |
|---|---|---|---|---|
| FO-1 | **High** | **Open 的 `LatencyCircuitBreaker` 從不改道流量**：成功路徑只呼叫 `RecordLatency`，`Select` 從不查 breaker，`ShouldFailover` 只在錯誤分支被問。 | T3：A 每筆成功但 2 ms、absMax 1 µs、threshold 1 → 5 筆後 LCB open、failures=5，**5/5 read 仍打 A**。`circuit_breaker_open` 事件文件叫操作員「Page — reads are being routed away」，是假的。 | (a) read path 在 `Select` 後檢查 optional `RouteAway(cluster) bool`，為 true 則換邊並呼叫一次 `OnFailure` 讓 sticky 跟上；或 (b) 文件改為「LCB 只 pre-arm failover」並修 event 文件。 |
| FO-2 | **High** | **`AdaptiveDualWrite` degraded 模式對同一叢集執行同一 statement 兩次**：fire-and-forget goroutine 執行 + caller 端因 `ErrWriteAsync` enqueue replay + `DefaultExecuteFunc` 再執行。 | T4：`ForceDegrade(B)` + `UPDATE counters SET hits = hits + 1` → B 收到 2 次。Counter、list append、任何非冪等 statement 在 degraded 期間**每筆**都 double-apply，這是設計不是 race。文件只警告 CounterBatch。 | `ErrWriteAsync` 的 replay 改在 fire-and-forget 完成回報失敗時才 enqueue（用 client 傳入的 callback）；或 `WithAdaptiveAsyncReplay(false)`；文件大聲標明 counter 必須 `Strict()`。 |
| FO-3 | **High** | **Auto-refresh 可能關掉健康的 session**：`lastSuccessNanos` 建構時不初始化 → 5 分鐘視窗從開機就滿足；`recordOpOutcomeAt` 把 schema error、invalid query、ctx error 全計入。 | T11：十筆 "Unconfigured table" INSERT 失敗（叢集可達）→ 下個 tick 呼叫 refresher，關掉原本好的 session A。生產情境：部署了對尚未 migrate 的 table 的 query，低流量服務 5 分鐘內沒有成功 read → 每 `MinRetryInterval` 拆一次好好的連線池。 | 建構與 `SwapSession` 時 `lastSuccessNanos = now`；只有連線類錯誤計入 refresh（提供 `WithAutoRefreshFailureClassifier(func(error) bool)`，因 Helix 不能 import driver error 型別）；舊 session 延遲關閉。 |
| FO-4 | Medium | **write 端沒有 per-cluster timeout**：`ConcurrentDualWrite` 等最慢那邊；`SyncDualWrite` A 慢完 5 秒後 `ctx.Err()` 短路 B → 兩邊都錯 → `DualClusterError` 且**不 replay**，A 實際結果未知。 | S4：A 5 秒 latency、無錯誤。 | `WithClusterWriteTimeout` 在 `executeDualWrite` 的 closure 內套用，慢的邊變成可 replay 的錯誤、快的邊 ack 保留。 |
| FO-5 | Medium | **`PrimaryOnlyRead` 恢復是 thundering herd**：timeout 到期後每個並發 caller 的 `Select` 都回 A 直到有人完成；A 卡住時整個 driver timeout 視窗內的 read 都被犧牲，每次失敗又重設 timer。 | | probe 單飛（CAS 一個 `probing` flag）。 |
| FO-6 | Medium | **Read strategy 狀態變化不可觀測**：`read_strategy.go` 沒有 logger/metrics/emitter；`EventFailover` 只在 failover 那一筆 read 發出（buffer 128，高負載會丟），之後所有 read 靜默去 B。沒有「preferred cluster」gauge。操作員無法從遙測回答「這個 client 現在黏在哪、為什麼搬」。 | | `SetReadPreferredCluster` gauge + `EventReadRouteChanged{From,To,Reason}` 從 strategy 發出（已有 `SetClusterNames` 注入路徑可沿用）。 |
| FO-7 | Medium | **Event 兩層 buffer 讓重要事件被噪音擠掉**（SUSPECTED）：per-read 的 `EventFailover`/`EventReadDivergence` 與罕見的 state transition 事件共用 128 slot、drop-newest；outage 時 breaker open 事件晚一微秒到就被丟。policy `eventOutbox` 的 drop 計數沒人讀。 | `events.go:21`、`policy/event_outbox.go:40,76,129,145` | per-kind 保留 slot 或 coalesce per-read 事件；outbox drop 併入 `cluster_events_dropped_total`。 |
| FO-8 | Low | `RefreshSession` 與操作員手動 `SwapSession` 交錯時，會關掉操作員剛換上的新 session（SUSPECTED）。 | | 用建構前擷取的 holder pointer 做 CAS，不同就關掉 refresher 建的那個並回傳錯誤。 |
| FO-9 | Low | CB gauge 在 StickyRead 放棄某叢集後永遠停在 "open"（timed close 只在 `RecordFailure` 裡評估）；CB × PrimaryOnlyRead probe 每個 cycle 兩筆使用者可見錯誤（實測）。 | | `ShouldFailover` 或定期 sweep 中 lazily 評估 reset；`ShouldFailover` 拒絕時仍通知 strategy `OnFailure` 讓它重設 timer。 |

---

## 5. 極端情境下的 auto recovery / heal

### 5.1 情境矩陣（預設設定：AdaptiveDualWrite + probe + StickyRead + CB + MemoryReplayer/Worker 或 NATSReplayer，auto-refresh 開啟）

| # | 情境 | 自動收斂? | 復原時間 | 資料風險 | 卡住的狀態 |
|---|---|---|---|---|---|
| S1 | A 硬掛 6 小時後回來 | **部分**：write 是、read 看 strategy、**資料否** | write：A 回來後 ≤10 s（5 個 probe tick）；read：StickyRead 永不回 A / PrimaryOnlyRead 等 recoveryTimeout | **高**：replay 在 enqueue 後 ~1.5 s（memory）或 5 次即時 Nak（NATS）就被丟；A 回來時 backlog 是空的，6 小時的 write 在 A 上永久缺失 | sticky preferred=B forever；A 的 CB gauge 永遠 "open"；沒有 backlog-aware read gating |
| S2 | 兩邊同時掛 2 分鐘 | **部分**：狀態對稱恢復，資料否 | write：3 筆失敗後兩邊 degraded，回來後各 ~10 s | **高**：兩邊 degraded 後每筆 write 回 `nil`，只靠 replay queue 撐（1.5 s 後丟）或沒設 Replayer 時什麼都沒有 | PrimaryOnlyRead 無 recoveryTimeout 時停在 B |
| S3 | A 每 10 s 掛 1 s，持續 1 小時 | **是**（狀態），但震盪 | 每次 blip 3 筆 write 內 degraded、5 筆快 write 後恢復 | 低–中：degraded 視窗內每筆 write 經 replay 重複（counter 除外冪等） | ~360 次 degrade/recover pair/小時，無時間性 hysteresis |
| S4 | A 活著但 5 s latency、無錯誤 | **部分** | Adaptive：3 筆慢 write（caller 可見 15 s）後 degraded；LCB 3 筆慢 read 後 open 但**不改道** | 中：replay worker 序列執行 → 0.2 筆/s，queue 滿、丟 | Concurrent/Sync 每筆 write block 5 s；probe（快的 `system.local` read）會讓「只有 write 慢」的叢集 10 s 內恢復 → 25 s 週期震盪，每週期 15 s caller 痛 |
| S5 | replay 飽和 / NATS 不可達 / poison | **部分** | — | **高**但大聲：queue 滿、enqueue 失敗都有 metric+event+callback。**靜默**：JetStream `DiscardOld` 淘汰、`MaxAge` 過期、replay 忽略原 consistency、TTL 漂移 | NATS outage 為每筆有失敗 leg 的 write 加上最多 5 s 同步等待 |
| S6 | Session 永久死亡（DNS/port 變更） | **部分**：`ConcurrentDualWrite` 可；**`AdaptiveDualWrite` + StickyRead（推薦組合）被餓死** | ≥5 min + ≤30 s tick；之後每分鐘重試 | 餓死時高：A 永遠 degraded，replay 打向死 session 全丟 | `consecutiveFailures` 凍結在 3 < threshold 10：`ErrWriteAsync` 不計、fire-and-forget 與 probe 的失敗都不進 `clusterStats`、sticky 在 B 所以沒有 read 碰 A。**實測：Concurrent 觸發 8 次 refresh，Adaptive 0 次** |
| S7 | NatsKV topology 失去 NATS / 兩邊 draining | **是**（drain 狀態） | NATS 回來後 ≤ PollInterval 5 s | 低：fail-closed 保留最後已知狀態 | watch 模式一旦掉到 poll 就永遠不回 watch；啟動時 NATS 不在則 fail-open |
| S8 | Process 重啟 | Memory：**否**（文件有）；NATS：**是** | NATS：立即（durable work-queue consumer） | Memory：只有 graceful Close 有 `OnDrop("shutdown")`，SIGKILL 無痕。NATS：batch 100 × 慢執行 > AckWait 30 s → 重送 + 重複執行 + 燒 MaxDeliver | — |
| S9 | 復原順序 / 操作員覆寫 | **否**（backlog 順序靠操作員，文件有說） | — | read 在 backfill 前回 A → stale read | **recovery probe 在 ~10 s 內默默撤銷 `ForceDegrade`**（實測 200 ms）；CB × PrimaryOnlyRead probe 每 cycle 2 筆錯誤 |
| S10 | 長時間 degraded 的 goroutine/timer | **是** | — | 低 | 全部有界（semaphore 100、retry pool 100、outbox 64、dispatcher 128）；只有 caller 不給 deadline 時 Concurrent 的 goroutine 會堆 |

### 5.2 發現

| ID | 嚴重度 | 問題 | 方向 |
|---|---|---|---|
| R-1 | **High** | **Replay 是秒級 retry buffer，不是 outage backlog**（摘要 #1）。Memory：`MaxAttempts=5`、backoff 100/200/400/800 ms = 1.5 s；retry pool 100 滿了之後**立即**丟（實測 500/500 在 t=1.50 s 前全丟，其中 400 筆只試了 1 次）。NATS：`Nak()` 無延遲、無 `BackOff`、`MaxDeliver=5` → `Term()`（實測即使設 `WithRetryDelay(2s)` 仍在 4.5 s、5 次後丟；該選項在 NATS backend 根本沒被引用）。CHANGELOG 記錄了 infinite→bounded 的行為變更，但預設值沒有為 outage 設計；`docs/replay-system.md:572` 說 NATS backoff「由 AckWait 控制」是錯的。**兩個 worker 都沒有「目標叢集掛了就暫停」的閘門。** | (1) replay 執行以目標叢集健康閘控：`IsDegraded(X)` 或 probe 失敗時暫停該叢集的 consumer，或把「unreachable」類錯誤視為不消耗嘗試次數；(2) NATS 用 `NakWithDelay(calculateBackoff(...))` 並設 consumer `BackOff`，`MaxDeliver` 預設大幅提高或 −1、以 `MaxAge` 當真正上限；(3) memory 把 payload 停泊而非重試；用 wall-clock budget 推導 `MaxAttempts`；pool 飽和時回到 queue 尾端而非丟；(4) 區分「target unreachable」（永不 terminate）與「statement rejected」（poison，terminate）；(5) 文件寫明每個 backend 的實際存活視窗。 |
| R-2 | **High** | **兩邊都 degraded ⇒ write 回 `nil` 但零份同步副本。** 3 筆失敗後 Adaptive 兩邊都 fire-and-forget → 兩邊 `ErrWriteAsync` → 都不是 real error → 回 `nil`，只靠 replay（R-1 之後 1.5 s 死）或沒設 Replayer 時什麼都沒有（建構時 `Warn` 一次）。實測：write 0–2 回 `DualClusterError`，write 3+ 回 `nil`。 | 兩邊都是 Async/Dropped 時回 `DualClusterError`（或新 sentinel `ErrNoSyncAck`）除非 Replayer 宣告 durable（介面 marker）；沒設 Replayer 時任何 async-only 結果必須是錯誤。預設 `Replayer==nil` 時的 partial failure 至少要發 `IncReplayDropped` + `EventReplayDropped(reason="no replayer")`。 |
| R-3 | **High** | **Auto-refresh 被 `AdaptiveDualWrite` 餓死**（S6）。推薦的生產組合下死掉的 session 永遠不會被 refresh。 | fire-and-forget 結果與 probe 結果回饋到 `clusterStats`（strategy→client callback，或 client 包住 `writeA/writeB` 觀察內層結果）；或讓 probe loop 的連續失敗數也能觸發 refresh。 |
| R-4 | **High** | **Recovery probe 默默撤銷 `ForceDegrade`**。`ForceDegrade` 只設 `isDegraded`；probe 只看 `IsDegraded`；`docs/auto-recovery.md` Phase 1 教的隔離流程在生產 ~10 s 內被撤銷，除非 `WithRecoveryProbeDisabled()`。fire-and-forget 成功也會 credit 恢復，所以 probe 關掉一個健康但正在 backfill 的 A 還是會自己恢復。 | `clusterWriteState` 加 manual latch，由 `ForceDegrade` 設、只有 `ForceRecover`/`Reset` 清；probe 與 `recordFast` 都不得清它（latched 時跳過 probe）；文件寫明互動。 |
| R-5 | Medium | **Probe 沒有 latency / 穩定期條件** → write-slow 叢集 25 s 週期震盪（S4）、flapping 叢集每 blip 一對事件（S3）。 | probe latency ≤ `absoluteMax`（或 ≤ sibling+delta）才 credit；最小 degraded dwell time / 指數 re-degrade backoff；發 "flapping" event/metric。 |
| R-6 | Medium | **NATS `BatchSize × latency > AckWait` ⇒ 重送風暴**：batch 序列執行、無 `InProgress()` heartbeat，慢的復原中叢集會被 ~3× replay 負載打。 | 每筆執行前 `msg.InProgress()`，或 `AckWait ≥ BatchSize × ExecuteTimeout`，或動態縮小 batch。 |
| R-7 | Medium | **Drain 是資料遺失視窗**：`executeWriteWithDrain` 把 replay enqueue 到 draining cluster，`DefaultExecuteFunc` 不查 drain，worker 立刻打 drained cluster 並燒掉 R-1 的預算。若 B 其實可達，replay 也違反 drain 契約。 | `Worker.PauseCluster(id)` 由 drain 轉換驅動；或 `DefaultExecuteFunc` 回 `ErrClusterDraining` 讓 backend 視為「長延遲 Nak、不計入 MaxDeliver」。 |
| R-8 | Medium | **沒有 replay-backlog-aware 的 read gating**：`PrimaryOnlyRead` recoveryTimeout 或 CB half-open 會在 backfill 前把 read 帶回 A → stale read。文件坦白說了並要操作員用 `WithAllowedClusters`，但 `MemoryReplayer.Len()` / consumer pending 都沒被任何路由碼消費。 | 內建 `helix.ExcludeWhileReplayBacklog(worker, threshold)` 之類的 `AllowedClusters` helper，讓「read 回 A」自動等 backlog 排空。 |
| R-9 | Low | 失敗的 probe 只 `Debug` log：6 小時 degraded = 10,800 次失敗無任何 `Warn`。 | 首次與 2 的冪次時 `Warn`（override error 已用此模式）。 |
| R-10 | Low | Topology watcher 掉到 poll 後永不回 watch；`StickyRead` 沒有自動回家路徑。 | 定期重試 `Watch`；可選 `WithStickyReadProbeBack(d)`，只在 failover policy 回報 closed 且 backlog 為空時回探。 |
| R-11 | Low | **Simulation 覆蓋缺口**：`complete-failure`（15 s outage）與 `replay-saturation` 用預設 worker（我已確認 `test/simulation/simulation.go:341` 與 `cmd/main.go:226` 都沒覆寫 `MaxAttempts`），並斷言 `nil`/`ErrWriteAsync` ack 的 key 兩邊都在。依 R-1 它們應該要 fail；不是今天就在 fail，就是 chaos path 遮掉了。 | 用 docker 跑 `-profile quick` 並在 `OnDrop` 加儀器確認。 |

---

## 6. Replay / Mirror 資料一致性

### 6.1 不變式檢查

| 不變式 | 成立? | 證據 |
|---|---|---|
| 每筆 write 都帶 client timestamp，replay 用同一個 | **是** | `getTimestamp` 永遠有值；兩邊 leg 與 replay 都 `WithTimestamp(ts)` |
| 部分失敗的 write 不是被 replay 就是告訴 caller | 有條件 | enqueue 失敗仍回 `nil`，只有 metric/log/callback/event；沒設 Replayer 只在建構時 warn |
| Replay 的 statement 與原始語意相同 | **否** | `ReplayPayload` 沒有 consistency / serial consistency；`USING TTL` 從 replay 時間重算；部分 Go 型別 encode 有損 |
| 非冪等 statement 不會被 replay 兩次 | **否** | counter / list append / CounterBatch 無偵測；`ErrWriteAsync` 在背景 write 仍在飛時就 enqueue（FO-2） |
| 已進 queue 的訊息會活到目標接受為止 | **否** | R-1 |
| Replay 尊重 drain | **否** | R-7 |
| Stream/queue 淘汰可觀測 | **否** | `MaxAge` 過期、`DiscardOld` 淘汰無任何訊號；`SetReplayQueueDepth` 沒有生產呼叫者 |
| Payload enqueue 後不可變 | 有條件 | NATS 在 enqueue 時 encode（安全）；**MemoryReplayer 直接持有 `q.values` 的參考不 clone**，mirror path 有 clone `[]byte` 但 replay path 沒有 |
| Graceful shutdown 不丟不重 | 有條件 | NATS：執行中的做完再 ack，其餘 Nak；Memory：全部 `OnDrop("shutdown")` |
| Replay 打到原本要打的叢集 | 有條件 | `DefaultExecuteFunc` 在執行時解析**當前** holder，`SwapSession` 後 backlog 跟著 slot 走；任何非 "A" 的 `TargetCluster` 字串都映射到 B |
| Mirror 有界且 drop 有計數 | **是** | `TryEnqueue` 非阻塞、drop 計數 + metric + rate-limited log |
| 操作員能量化每叢集資料缺口 | **否** | 只有 counter；`replay_queue_depth` 永遠 0；沒有 oldest-age；`Pending()` 是 stream-wide |

### 6.2 發現

| ID | 嚴重度 | 問題 | 方向 |
|---|---|---|---|
| D-1 | **High** | R-1 的 NATS/Memory 預算問題（同一件事，資料一致性視角）。 | 同 R-1 |
| D-2 | **High** | **Stream 層靜默遺失**：`MaxAge 24h`、`DiscardOld`、預設 `Replicas: 1` on FileStorage（單節點磁碟壞 = backlog 全沒）。 | 預設 `DiscardNew`（fail loud）或輪詢 `stream.Info()` 的 `FirstSeq` 移動發 `EventReplayEvicted`；`Replicas` 預設 3 或啟動 warn。 |
| D-3 | Medium | **Args encode 有損或拒絕常見型別**（NATS path，實測）：`*big.Int`、`*inf.Dec`、UDT struct、`map[int]…` → encode error → enqueue 失敗 → write 丟；`net.IP` → `[]any{uint64…}` → 每次 replay 都 driver error → poison；**空 `[]byte{}` 解碼成 `nil` → replay 寫入 NULL tombstone**。MemoryReplayer 什麼都收，換 backend 才爆。 | 為 `big.Int`、`inf.Dec`、`net.IP`、`gocql.Duration` 加明確 encoder；「不支援型別」清單在**兩個 backend 的 Enqueue 時**檢查；空 `[]byte` 與 nil 區分。 |
| D-4 | Medium | **`replay_queue_depth` 是死 gauge**，沒有 oldest-message age、沒有 per-cluster/priority pending；`replay_dropped_total` 混合 enqueue 失敗與 worker 耗盡。 | worker 呼叫 `SetReplayQueueDepth`（memory：`Len()`；NATS：每次 poll 的 `consumer.Info().NumPending`）；加 `replay_oldest_age_seconds{cluster}`；dropped 加 reason label。 |
| D-5 | Medium | **MemoryReplayer 持有 caller arg slice 的參考**：app 用 `sync.Pool` 的 `[]byte` 做 blob → Exec 回 nil 後 buffer 被重用 → 200 ms 後 B 的 replay 用**下一列的 bytes** 寫在**原本的 key** 下。 | 在 `enqueueReplayIfNeeded` 重用 `cloneArgs`/`cloneBatchEntries`（只在失敗路徑，hot path 零成本）。 |
| D-6 | Medium | Replay 不保留 consistency level / serial consistency；TTL 時鐘重算。 | `ReplayPayload` 加 `Consistency`（msgp 訊息同步加）；文件寫明 TTL 漂移。 |
| D-7 | Low | 沒有 `Nats-Msg-Id` dedup：publish 在 server 存好後才 timeout → 計為 `replay_dropped` 但其實會 replay（metric 誤導）。 | 用 timestamp+cluster+hash 當 msg id。 |
| D-8 | Low | Poison message 燒滿預算且 metrics 看不見；`Term()` 本身失敗時 `IncReplayDropped`/`OnDrop` 被跳過，遺失無計數。 | 補 metric；`Term` 失敗也計數。 |
| D-9 | Low | Mirror `Stop` 在 `client.Close()` 內同步排空 8192 slot 的 queue；publisher 模式每筆 5 s timeout → `Close()` 可能卡數分鐘，文件沒說。 | 文件 + 可選 drain timeout。 |
| D-10 | Low | 文件/程式漂移：`worker.go:31` 說有 jitter（沒有）；`RetryDelay/MaxRetryDelay` 對 NATS worker 接受並驗證但沒用；`vm/doc.go:77` 宣傳永遠 0 的 gauge。 | 修文件或實作。 |

---

## 7. 整合後的改善路線圖

依「對生產資料安全的影響 / 實作成本」排序。同一個 root cause 的項目已合併。

### P0 — 資料遺失與靜默錯誤（建議下一個 release 前處理）

| # | 工作 | 關閉的發現 | 規模 |
|---|---|---|---|
| 1 | **Replay 預算改為 outage-survivable**：NATS `NakWithDelay` + consumer `BackOff` + `MaxDeliver` 大幅提高；Memory 改 wall-clock budget、pool 飽和時不丟；兩者加「目標 unreachable 不消耗嘗試」的分類；worker 依 `IsDegraded`/drain 暫停該叢集。修正 `docs/replay-system.md` 與 `docs/auto-recovery.md`。 | R-1, D-1, R-7, D-10 | 中（replay 套件內，root 只加 pause hook） |
| 2 | **ctx 錯誤統一不計健康**：`classifyReadErr` 單一分類函式，六個 read 入口與 `recordOpOutcomeAt`、`tryFallbackCluster`、`adaptive_write.isSkippedErr` 都走它；`ctx.Err()!=nil` 時跳過 failover。 | FB-1, A-1（部分）, FO-3（部分） | 小–中 |
| 3 | **兩邊 async-only 不得回 `nil`**；`Replayer==nil` 的 partial failure 發 `EventReplayDropped(reason)`。 | R-2 | 小 |
| 4 | **Auto-refresh 修正**：建構/swap 時 seed `lastSuccessNanos`；fire-and-forget 與 probe 結果回饋 `clusterStats`；錯誤分類器 option。 | FO-3, R-3 | 小–中 |
| 5 | **`ForceDegrade` manual latch**，probe 與 `recordFast` 不得清除。 | R-4 | 小 |
| 6 | **MemoryReplayer clone args**；`ErrWriteAsync` 的 replay 延後到背景 write 回報失敗時。 | D-5, FO-2 | 小 / 中 |
| 7 | **Stream 層可觀測**：`DiscardNew` 預設或 eviction event；`Replicas` 預設 3 或 warn；`SetReplayQueueDepth` 接上生產呼叫者；加 oldest-age gauge。 | D-2, D-4 | 小–中 |

### P1 — 正確性與可操作性

| # | 工作 | 關閉的發現 |
|---|---|---|
| 8 | LCB open 時 read 改道（`RouteAway` optional interface），或改文件為「pre-arm」並修 event 文件 | FO-1 |
| 9 | CB 拆「可否重試」與「是否換 preferred」；half-open 改背景 probe | FB-3, FO-9 |
| 10 | `Iter`+`PageState` 釘住叢集；`Iter.Close` 錯誤餵 breaker/strategy；`Iter`/CAS drain-aware | FB-2, FB-5, A-6 |
| 11 | StickyRead：現任 preferred 失敗時允許 cooldown 內切換；`Reset/SetPreferred`；preferred gauge + `EventReadRouteChanged` | FB-4, FO-6 |
| 12 | Per-cluster write timeout option | FO-4 |
| 13 | Probe latency/dwell 條件（hysteresis） | R-5 |
| 14 | NATS `InProgress()` heartbeat 或 AckWait/BatchSize 關係 | R-6 |
| 15 | Args encoder 補齊 + 兩 backend 一致的型別檢查；`Consistency` 進 payload | D-3, D-6 |
| 16 | Backlog-aware `AllowedClusters` helper | R-8 |
| 17 | `root_validation.go` 補三條組合檢查；`Config()` 回傳 copy | A-7, A-4 |

### P2 — 效能與結構

| # | 工作 | 關閉的發現 |
|---|---|---|
| 18 | Replay executor pool（兩 backend）+ per-cluster queue | P-1, R-1（吞吐面） |
| 19 | NATS enqueue async option（PublishAsync + bounded pending）或 staging ring | P-2 |
| 20 | `cql_client.go` Tier 1 檔案拆分 → Tier 2 `readRouter`/`writeOrchestrator`/drain-as-sentinel | A-2, A-5 |
| 21 | CB `RecordSuccess` fast path；Adaptive strike atomic + fast path；vm histogram 無鎖；`dualWriteJob` struct；setter bitmask；v1 adapter `Release()` | P-3 ~ P-8 |
| 22 | 匯出能力介面 + policy 編譯期斷言；event per-kind 保留 slot；outbox drop 併入 metric | A-3, FO-7 |
| 23 | `Close` join auto-refresh/topology goroutine；`RefreshSession` vs `SwapSession` CAS | A-1(F-08), FO-8 |
| 24 | 文件漂移清理（A-10, D-10, FB-9）；simulation/e2e 進 nightly CI；read 分類矩陣改 table-driven test | A-10, R-11 |

---

## 8. 需要作者釐清的設計意圖

1. **`MaxDeliver`/`MaxAttempts` 的契約是 poison cap 還是 outage budget？** 程式實作前者、預設值讓後者不可能、文件描述後者。若兩者都要，就需要 unreachable vs rejected 的錯誤分類。
2. **兩邊 async-only 回 `nil` 是否刻意？** 若是為了 NATS durable 情境，應該以 Replayer durability marker 為條件而非無條件。
3. **`DeadlineExceeded` 是否刻意當成 AdaptiveDualWrite / LCB 的 latency 訊號？** slice path 的例外暗示不是，但 write path 可能是故意留的。
4. **`ForceDegrade` 應該 sticky 嗎？** 文件把它當隔離步驟，但 probe 會撤銷。
5. **Read 端 CircuitBreaker 的目的是保護叢集（做不到，流量沒被擋）還是保護 caller（反而增加錯誤）？** 若意圖是閘控 strategy 狀態變化而非重試，P1-9 的拆法一次解 FO-1 與 FB-3。
6. **`Iter` 忽略 drain 是否刻意？** 若理由是 paging cursor 要留在同一叢集，只該在 `PageState` 設定時跳過改選。
7. **`SwapSession` 後 backlog 跟著 slot 走是否為預期語意？** 或應以 session generation 圍籬？
8. **`Replicas: 1` 預設是否刻意？** 與「生產推薦 NATSReplayer」矛盾。
9. **`topology` 為何 import root 而非 `types`？** 搬到 `types` 可讓 `topology` 與 `policy` 平行、root 得以引用 topology 型別。
10. **Simulation `complete-failure` 今天是否通過？**（R-11）

---

## 9. 驗證狀態

- 各 agent 以 throwaway test / benchmark 重現：replay 3 個（arg round-trip、NATS Nak storm、memory outage window）、fallback 10 個（`-race` 全過）、recovery 6 個、perf 一組 scratch benchmark + escape analysis + memprofile。程式碼皆在 scratchpad，未進 repo。
- 我另外親自抽查確認：`nats_worker.go:256-261` 的 `Nak()` 無延遲且 `BackOff/RetryDelay` 在 NATS backend 無引用；`DefaultExecuteFunc` 無 drain 檢查；`isReadTerminalNonHealth` 未排除 ctx 錯誤；`cql_client.go` 無任何 `IsOpen` 類查詢（read routing 從不看 breaker）；`lastSuccessNanos` 只在 `recordOpOutcomeAt` 寫入；`DefaultWorkerConfig` `MaxAttempts=5` 且 retry pool 飽和即丟；`executeDualWrite` 兩邊 async 回 `nil`；probe 只閘 `IsDegraded`、`ForceDegrade` 只寫 `isDegraded`；auto-refresh skip list 含 `ErrWriteAsync`；simulation 未覆寫 `MaxAttempts`。
- 未執行：任何需要 docker 的 integration / simulation / e2e；真實 gocql session 的 adapter benchmark（P-8 標 SUSPECTED）。
- Repo 未被修改（`.gitignore` 的 M 是 review 開始前就存在的）。
