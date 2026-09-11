# lcache —— 无锁哈希之上的本地缓存语义中间件

> **L**ocal **Cache**：一个纯 JDK、零三方运行依赖的**进程内本地缓存（L1）中间件**。
> 数据面复用论文级无锁哈希表（SP Concurrent Size），缓存语义（驱逐/TTL/失效/读穿/统计）
> 全部自研。设计主线是**无锁编程 + JMM 内存模型**：读路径无锁直读、写路径线程亲和收口。

- 本仓库主线回答"**并发内核怎么写**"：CAS、volatile、happens-before、线性化点，
  每处关键并发逻辑都有注释与测试背书。

---

## 1. 特性矩阵

| 能力 | 说明 |
|---|---|
| 数据面引擎可插拔 | `LOCK_FREE`（默认，SP 无锁哈希 + per-thread 精确 size）vs `SYNCHRONIZED`（全锁基线，供对比） |
| 精确 size() | 无锁引擎由 SP per-thread 计数器求和：O(写线程数)、无锁、不阻塞写 |
| 容量上限 + 驱逐 | `maxSize` + `FIFO` / 近似 `LRU`（读计数 + 驱逐点 CLOCK 二次机会），`RemovalListener(EVICTED)` |
| per-key TTL | `expireAfterWrite`：读/写触发惰性判定(miss) + 写池时间闸清扫，`RemovalListener(EXPIRED)` |
| 显式失效 | `invalidate(key)` —— "写库后失效缓存"一致性用法 |
| 读穿 + 单飞加载 | `get(key, loader)`：未命中单飞加载，防缓存击穿/惊群 |
| 统计 | 命中/未命中/驱逐/过期/加载次数，`CacheStats` 快照（LongAdder 无锁计数） |
| 可注入时钟 | 过期行为可用假时钟做确定性测试（无 sleep） |
| 无持久化 / 无网络层 | 纯内存、纯 JDK、库形态（可作依赖被其它模块引用） |

---

## 2. 架构总览

```
                业务线程（不可控、任意多个）
                        │
        ┌───────────────┴───────────────┐
        │  LocalCache 门面                │
  读：get/getIfPresent/containsKey  写：put/invalidate/get(K,loader)
        │                                │
        │ 无锁直读（任意线程）             │ 收口：submitAndAwait
        ▼                                ▼
   CacheEngine.get()             MutationExecutor（线程亲和写池，≤64 线程）
        │                                │
        ▼                                ▼
┌────────────────────────┐      ┌──────────────────────────────┐
│ SP 无锁哈希 SizeHashTable│      │ 写线程(绑定 ThreadID 槽位)执行:  │
│   ·桶内有序无锁链表(CAS)  │      │  engine.put/remove (SP 计数内联) │
│   ·per-thread size 计数  │      │  顺序链表(驱逐/TTL)维护 @ orderLock│
└────────────────────────┘      │  驱逐(CLOCK 二次机会)/过期清扫   │
        ▲                        └──────────────────────────────┘
        │ CacheNode（value/expireAt/reads/链表链接）被哈希桶与顺序链表共同引用
        └──────────── 顺序链表 head..tail：驱逐/过期的淘汰依据
```

**为什么"读直读、写收口"**：
- SP 无锁引擎对**读是纯 volatile 读**，任何线程可调用；
- SP 对**写要求当前线程已注册 per-thread 计数槽（≤64）**。业务线程不可控且可能超 64，
  因此写路径一律汇入固定写线程池（每个写线程绑定唯一稳定的槽位），正确性由"槽位绑定"保证；
- 结果是读吞吐不被写线程数约束，写正确性由固定槽位 + 底层 CAS 保证。

**为什么驱逐只在写侧维护**：若每次读都移动 LRU 链表，读路径就要写共享内存，违背"读无锁"。
近似 LRU 方案：**读只做一个原子自增（`CacheNode.reads`），不触碰链表**；提升推迟到
**驱逐决策点**才兑现——驱逐时对"近期被读"的队头做**有界 CLOCK 二次机会**（复位计数、转队尾
继续探测，上限为常数 `CLOCK_PROBES`），写成本 O(1) 且与驻留集规模无关。显式 `cleanUp()` 仍做
整链提升（完整维护语义）。这一改动源于一次写吞吐骤降回归，详见下方 §2"LRU 提升代价修复"。

**一致性口径（宽松，后端缓存惯例）**：读可能短暂看到"已驱逐但早已握住的旧节点"或"已过期但
未清扫的条目"；读到过期值一律视为 miss，由维护任务异步收敛。

### 写池过载保护：无界队列 → 有界准入闸（2026-09 修改）

**改动前**：`MutationExecutor` 用 `new LinkedBlockingQueue<>()`（无界，默认容量 `Integer.MAX_VALUE`），
且 `corePoolSize == maximumPoolSize == writerThreads` → 队列永远不满、池永不饱和、永不拒绝。
后果是：一旦写路径吞吐塌掉（写线程被卡死/变慢），多余写任务**无限积压进堆内存**，表现为"调用方无限
阻塞 + 队列悄悄增长，直到某天 OOM"——出事前完全不可见、事后极难归因。

**改动后（三件事）**：
1. **有界准入闸 `Semaphore(64)`**：非写池的调用方提交前先 `tryAcquire` 拿票、任务完成/失败后
   `release` 还票，排队+执行中的写提交总数被 64 封顶；写线程自身重入时**就地执行、不拿票**
   （避免自己等自己死锁）。任务被拒发生在入队**之前**、尚未执行，因此重试/上层重试均安全。
2. **任务队列有界 `ArrayBlockingQueue(64 + 1)`**：
   - `64` = 闸上限。排队写任务数不可能超过"闸内任务总数（≤64）"，队列再大也装不满，纯浪费内存；
   - `+1` = 给**维护任务**留的空位。维护任务经 `drainGate` 合并闸限流、最多 1 个在排，且**不经
     准入闸**直接入队；若队列被写任务占满，它进不来会把执行到一半的 `put` 打断抛错，故恒留
     1 个空位保证其畅通。
     （2026-09 更新：经 `drainGate` 排队的周期维护现仅指**过期清扫**；LRU 提升与容量驱逐已分别
     改为驱逐点 CLOCK 二次机会与 put 内联，不再经此排队——见下节。）
3. **过载快速失败 + 有限重试**：拿票超时（默认 5s）→ 暂停 200ms 重试 1 次 → 仍失败则打一条诊断日志
   （JUL，带当前排队数/队列长度）并抛 `RejectedExecutionException`。极端下调用方最长等待
   ≈ `2×5s + 200ms ≈ 10s` 后快速失败。

**为什么这样改**：
- **防 OOM**：写任务的内存占用不再无上限，"排队内存"封顶为 64 个任务 + 1 个维护位；
- **快速失败、失败可见**：把"写线程卡死"这类回归从"沉默到 OOM 才炸"变成"过载即有明确异常 + 日志"，
  符合后端惯例——宁可让调用方明确知道失败、自行重试/降级，也不静默无限积压；
- **为何不用教科书解法 `CallerRunsPolicy`**：它会把写操作放到**未注册 SP 计数槽**的业务线程上就地
  执行，直接破坏"写必须由 ≤64 个槽位绑定的写线程执行"这一核心设计前提，故此处不适用。

**可调参数**（`MutationExecutor` 顶部常量）：`DEFAULT_MAX_PENDING_WRITES=64`（闸上限）、
`SUBMIT_TIMEOUT_NANOS=5s`（单次等票超时）、`MAX_SUBMIT_ATTEMPTS=2`（含首试的重试总次数）、
`RETRY_PAUSE_MILLIS=200ms`。

**行为变化（注意）**：极端过载时 `put / putIfAbsent / invalidate / cleanUp` 可能抛
`RejectedExecutionException` 且**该次写不生效**；正常负载不受影响——单次写微秒级、清得飞快，
64 的闸在常规/秒杀级流量下几乎不会被填满。

### LRU 提升代价修复：写吞吐骤降 ~70× → CLOCK 二次机会驱逐（2026-09 修改）

**问题（工业形态基准暴露）**：给缓存配置 `maxSize > 0`（LRU 维护开启）后，写吞吐从 ~170 ops/ms
崩到 ~2.5 ops/ms；且**纯更新场景（`putUpdates`，零驱逐）同样崩**——初步判断指向"驱逐成本"是错的。

**根因（不是驱逐，是"每写一次全链扫描"）**：
1. `put0 / putIfAbsent0` 末尾无条件调 `maybeScheduleDrain()`（`LocalCacheImpl`）；
2. 只要 `promoteReads()` 为真（=`maxSize>0 && LRU`）就走维护调度；
3. 该调度经 `mutator.execute(...)`：因 put 正在**写池线程**上执行，`execute` 检测到
   `onPool` 后**就地内联**跑 `runMaintenance()`（`MutationExecutor.execute`），内含
   `drainPromotions()`：持 `orderLock` 把整条顺序链扫一遍、逐节点 `getAndResetReads()`；
4. 净效果：**每个 put 同步做一次 O(驻留集) 的全链扫描**——即使读计数全为 0（扫描零成果也照扫）。
   写成本从 O(1) 变 O(n)，骤降即此。

**修改点**（全部在 `lcache-core`，`LocalCacheImpl.java`；`CacheNode` 零改动）：
1. `evictExcess()` 改为 **CLOCK 二次机会驱逐**：驱逐时对队头若"近期被读"（`getAndResetReads()>0`）
   就复位计数并转队尾、继续探测，上限 `CLOCK_PROBES = 8`；否则淘汰。整段在 `orderLock` 下，
   `removeIfValue` 的"防误删并发替换出的新节点"语义原样保留。FIFO 不记读计数 → 探测恒判未读，
   退化为纯队头淘汰，行为不变。
2. 写路径不再调度"整链提升维护"：`put/putIfAbsent` 末尾不再无条件调度维护，驱逐顺序交由
   驱逐点的 CLOCK 探测即时兑现。纯 LRU 无 TTL → **零调度、零扫描**。
3. 原 `maybeScheduleDrain()` 收窄为 `maybeScheduleExpirySweep()`（仅 TTL 生效）：用**合并闸
   `drainGate`（最多 1 个在排）+ 时间闸**（距上次实际清扫不足 `expirySweepIntervalNanos` 的触发
   no-op）把偶发的 O(驻留集) 清扫摊销到间隔之上；间隔按 `ttl/8` 派生并夹在 `[1ms, 1s]`。
   `expireSweep()` 开头记录 `lastExpirySweepNanos`。
4. `drainPromotions()` / `runMaintenance()` 保留，但只在显式 `cleanUp()` 走（整链提升的完整语义，
   `EvictionPolicyTest` 依赖其确定性行为）。

**为什么这样改（符合后端场景）**：读路径依旧只做一个原子自增、不触碰链表；提升只在
**驱逐决策点**（唯一需要新鲜 LRU 次序的地方）以常数步数兑现，写成本与驻留集规模解耦；
等效于 OS 页缓存 / 生产缓存常见的 CLOCK 二次机会，避免了"周期整链扫描"这一 O(n)/写 的病态。
读与 `getAndResetReads` 的竞态属宽松 LRU 近似口径，可忽略。

**代价/口径变化**：
- LRU 从"维护期整链提升"改为"驱逐点 CLOCK 补偿 + cleanUp 显式整链提升"；驱逐时的淘汰次序仍
  即时反映读计数，命中率基本不变（数据见 `industrial_scale_report.md` §3）；
- 长期只读且从不驱逐/cleanUp 时，单节点 `reads` 可累计（需单 key >2^31 次读才回绕，现实可忽略，
  回绕只影响一次 CLOCK 判定）。


---

## 3. JMM / 无锁 关键点（代码内都有对应注释）

| 关注点 | 位置 | 处理 |
|---|---|---|
| 数据发布 happens-before | `CacheNode.value/expireAt` → `engine.put` | 构造后先写，经无锁表 CAS/volatile 发布；读者读到节点即见构造值 |
| 读计数原子自增 | `CacheNode.recordRead()` | `VarHandle.getAndAdd`，读路径唯一共享写 |
| 线性化点 | `put/remove` | 底层无锁表对 value 的 CAS 成功那刻 |
| 幂等摘链 | `unlink()` + `CacheNode.linked` | 多路径（驱逐/过期/失效）可能指向同节点，用布尔位防止重复 notify |
| 防误删新条目 | `CacheEngine.removeIfValue(key, node)` | 驱逐/过期只删除"当前 == 目标节点"的映射，避免删掉并发替换出的新值 |
| volatile 状态可见 | `MutationExecutor.onPool`、`closed` | 收口/重入检测与关闭标记 |
| 单飞加载协调 | `get(K,loader)` 的 `inflightLoads` | `ConcurrentHashMap` 登记进行中 Future，leader 执行 loader、其余等待 |
| 写池背压/防 OOM | `MutationExecutor` 准入闸 `Semaphore(64)` + 有界队列 `ArrayBlockingQueue(65)` | 非池调用方持票提交、池线程重入就地执行；拿票超时 → 重试 → 打日志并抛 `RejectedExecutionException` 快速失败（详见 §2"写池过载保护"） |
| LRU 提升 = 驱逐点 CLOCK 二次机会 | `evictExcess()` + `CacheNode.getAndResetReads` | 队头近期被读 → 复位计数、转队尾继续探测，上限 `CLOCK_PROBES=8`；读路径零改动、写成本 O(1)（详见 §2"LRU 提升代价修复"） |

---

## 4. 目录结构

```
croq-project/
├── pom.xml                    父 pom（JDK17 多模块聚合）
├── modules/
│   ├── sp-size/               嵌入式 SP 无锁 size 算法 vendor（无逻辑修改，见 VENDOR.md）
│   ├── lcache-core/           缓存语义中间件主体（纯 JDK）
│   │   └── src/main/java/io/lcache/
│   │       ├── LocalCache.java         门面接口 + builder()
│   │       ├── CacheBuilder.java       配置器（capacity/maxSize/TTL/policy/engine/...）
│   │       ├── EngineKind/PolicyKind/RemovalCause/RemovalListener/Clock/CacheStats
│   │       ├── engine/                 CacheEngine SPI + LockFree/Synchronized 实现
│   │       └── internal/               LocalCacheImpl、CacheNode、MutationExecutor、
│   │                                   StatsCounter、ThreadSlots
│   ├── lcache-benchmark/       JMH 基准（引擎吞吐/size + 工业形态 Zipf/驱逐 + footprint 内存探测）
│   └── lcache-demo/            后端请求场景演示（进程内，无网络）
├── run_industrial.sh          工业形态基准一键运行脚本（HotReadZipf / WriteEviction / FootprintProbe）
├── _archive/                  裁剪前的研究仓库（sp-core 全量、croq-core、papers 等）归档
└── README.md
```

---

## 5. 构建与运行

环境：JDK 17+，Maven 3.8+（本项目无三方运行依赖；测试用 JUnit5、基准用 JMH）。

```bash
# 全量测试（含 sp-size 零测试 + lcache-core 8 类 23 个用例）
mvn -q test

# 只跑核心模块测试
mvn -q -pl modules/lcache-core -am test

# 后端场景演示
mvn -q -pl modules/lcache-demo -am exec:java        # 若父模块也执行 exec 报错，可用：
java -cp modules/sp-size/target/classes:modules/lcache-core/target/classes:modules/lcache-demo/target/classes \
     io.lcache.demo.DemoRunner

# JMH：打包 fat jar 后按基准全限定名运行（本机不要求 --add-exports，见 VENDOR.md）
mvn -q -pl modules/lcache-benchmark -am package
java -jar modules/lcache-benchmark/target/lcache-benchmarks.jar \
     io.lcache.benchmark.EngineThroughputBenchmark -f 1 -wi 3 -w 2s -i 5 -r 2s -t 4

# 工业形态基准一键运行（读多写少 Zipf / 写密集驱逐 / -prof gc / -t 线程扫描 / footprint），
# 内含稳定旗标与矩阵收敛建议，建议直接执行：
bash run_industrial.sh

# 或按方法/参数单跑（命中率诊断打在 stderr 的 [hot-read]/[write-evict] 行）：
java -jar modules/lcache-benchmark/target/lcache-benchmarks.jar \
     io.lcache.benchmark.HotReadZipfBenchmark.mixed95_5 \
     -p engine=LOCK_FREE,SYNCHRONIZED -p keyModel=INT,STRING -p maxSize=4096,16384 \
     -t 8 -wi 5 -w 2s -i 5 -r 2s -f 1
java -jar modules/lcache-benchmark/target/lcache-benchmarks.jar \
     io.lcache.benchmark.WriteEvictionBenchmark.putCycling \
     -p engine=LOCK_FREE -p maxSize=16384 -p writerThreads=4 -t 8 -wi 3 -w 2s -i 5 -r 2s -f 1

# 内存占用探测（非 JMH，独立 main；建议按提示加大堆）
java -Xms2g -Xmx2g -XX:+UseSerialGC -XX:+AlwaysPreTouch \
     -cp modules/lcache-benchmark/target/lcache-benchmarks.jar \
     io.lcache.benchmark.FootprintProbe
```

快速示例：

```java
LocalCache<String, String> cache = LocalCache.<String, String>builder()
        .capacity(1 << 16)                       // 无锁哈希定容
        .maxSize(10_000)
        .policy(PolicyKind.LRU)
        .expireAfterWrite(10, TimeUnit.SECONDS)
        .engine(EngineKind.LOCK_FREE)            // 默认无锁；可换 SYNCHRONIZED 对比
        .removalListener((k, v, cause) -> log("remove", k, cause))
        .recordStats()
        .writerThreads(4)                        // 写线程池 ≤64
        .build();

cache.put("k", "v");
cache.get("k");                                  // hit
cache.get("absent", key -> db.load(key));        // read-through + 单飞加载
cache.invalidate("k");                           // 写库后失效
cache.size();                                    // 精确 size（SP per-thread 计数）
cache.stats().hitRate();
```

---

## 6. 测试与基准数据

README **不再内嵌测试/基准结果**，实测数据单独维护在一处，避免与代码注释、历史快照互相矛盾：

| 产物 | 位置 | 内容 |
|---|---|---|
| 单测 | `mvn -q -pl modules/lcache-core test` | 8 个测试类 / 23 个用例（并发一致性、驱逐策略、TTL 过期、单飞加载、写池过载故障注入等），逐类覆盖明细见报告 §1 |
| 测试报告（单测 + 全量基准） | [`industrial_scale_report.md`](industrial_scale_report.md) | §1 单元测试；§3–§10 八个基准模块，每模块含「测什么 / 参数 / 数据 / 结论」；§11 结论与后续 |
| 复跑入口 | `bash run_industrial.sh`（`QUICK=1` 为冒烟档） | 一键跑全部基准并生成报告表 |

> 报告对每个章节标注**结论可用性**（可用 / 需按口径重读 / 判为无效 / 外推）。
> 引用任何数字前，请先读该报告的 §0 口径约定与各节的「读数」。

---

## 7. 边界与使用条件（诚实清单）

1. **key 需可排序**：底层 SP 无锁表用有序链表组织桶 → key 需实现 `Comparable`，或 `builder.comparator(...)`；
   null 键/值被禁止。
2. **定容不可扩容**：`capacity` 在构建时确定（无锁哈希不可扩容）。语义层只承诺驱逐到 `maxSize`；
   若无需容量控制请给足 `capacity`。
3. **写线程 ≤ 64**：写路径收口到线程亲和池（`writerThreads` ∈ [1,64]），因为 SP per-thread 槽位上限 64。
4. **size() 含未清扫的过期条目**：过期条目在"判定 miss"与"清扫移除"之间仍被计数；过期清扫按
   **时间闸摊销**触发（间隔 ≈ `ttl/8` 夹在 `[1ms, 1s]`），`cleanUp()` 为同步强制清扫 → 其后精确。
5. **宽松一致性**：读与写并发时可能短暂读到旧值/过期值，按 miss 处理；不提供强线性化的 `getAndCompute` 之类。
6. **写为同步收口 + 过载有界**：`put/invalidate` 阻塞等待写池执行，提交经有界准入闸（`Semaphore(64)`）；
   极端过载（写线程长时间无法腾出闸位）时**快速失败抛 `RejectedExecutionException`，该次写不生效**，
   调用方需自行决定重试/降级（机制见 §2"写池过载保护"）。适合"读多写少 + 数据量可控"的 L1 场景，
   不适合海量纯写或要求写端超低延迟的极端场景。
7. **无持久化 / 崩溃不恢复 / 无网络层**：本缓存是**纯内存进程内组件**，不做 KV 落盘与多活。
8. **裁剪来源**：无锁哈希来自 SP 开源实现（见 `modules/sp-size/VENDOR.md`）；上层缓存语义与
   线程收口为本项目自研。
9. **近似 LRU = 驱逐点 CLOCK 二次机会**：提升只在驱逐决策点兑现（驱逐时对"近期被读"队头复位
   计数并转队尾，上限 `CLOCK_PROBES=8`；`cleanUp()` 才做整链提升）。"驱逐次序反映读"是在
   **驱逐发生时**成立的近似而非维护期即时序；写成本 O(1)、与 `maxSize` 无关。长期只读且从不
   驱逐/`cleanUp` 时单节点读计数理论上可累计（需单 key 超 2^31 次读才回绕，可忽略，回绕只影响
   一次 CLOCK 判定）。


---

## 8. 上游与许可

- 无锁哈希/SP 计数子集来源见 `modules/sp-size/VENDOR.md`（GPL v3 头保留在各文件）。
- 本模块 `lcache-core`（缓存语义、并发收口、驱逐/TTL/读穿/统计）为原创实现，Apache-2.0 语义的开源可自行选用。
