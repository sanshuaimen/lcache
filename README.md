# lcache —— 无锁哈希之上的本地缓存语义中间件

> **L**ocal **Cache**：一个纯 JDK、零三方运行依赖的**进程内本地缓存（L1）中间件**。
> 数据面复用论文级无锁哈希表（SP Concurrent Size），缓存语义（驱逐/TTL/失效/读穿/统计）
> 全部自研。设计主线是**无锁编程 + JMM 内存模型**：读路径无锁直读、写路径线程亲和收口。

**当后端进程内不能依赖 Redis（L2）时，我自己怎么写这一层 L1 本地缓存内核。**

- 本仓库主线回答"**并发内核怎么写**"：CAS、volatile、happens-before、线性化点，
  每处关键并发逻辑都有注释与测试背书。

---

## 1. 特性矩阵

| 能力 | 说明 |
|---|---|
| 数据面引擎可插拔 | `LOCK_FREE`（默认，SP 无锁哈希 + per-thread 精确 size）vs `SYNCHRONIZED`（全锁基线，供对比） |
| 精确 size() | 无锁引擎由 SP per-thread 计数器求和：O(写线程数)、无锁、不阻塞写 |
| 容量上限 + 驱逐 | `maxSize` + `FIFO` / 近似 `LRU`（读计数采样 + 周期性提升），`RemovalListener(EVICTED)` |
| per-key TTL | `expireAfterWrite`：读时惰性判过期(miss) + 写池周期清扫，`RemovalListener(EXPIRED)` |
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
└────────────────────────┘      │  驱逐/过期清扫/读计数提升        │
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
近似 LRU 方案：**读只做一个原子自增（`CacheNode.reads`），由维护任务周期性把高频节点提升到
队尾再淘汰队头**——读路径零链表操作（Caffeine 同源思路：读事件缓冲 + 异步维护）。

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
   - `+1` = 给**维护任务**留的空位。维护任务（过期清扫 / LRU 提升 / 容量驱逐）经 `drainGate`
     合并闸限流、最多 1 个在排，且**不经准入闸**直接入队；若队列被写任务占满，它进不来会把执行到
     一半的 `put` 打断抛错，故恒留 1 个空位保证其畅通。
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
│   ├── lcache-benchmark/       JMH 基准（引擎吞吐 / size 成本）
│   └── lcache-demo/            后端请求场景演示（进程内，无网络）
├── _archive/                  裁剪前的研究仓库（sp-core 全量、croq-core、papers 等）归档
└── README.md
```

---

## 5. 构建与运行

环境：JDK 17+，Maven 3.8+（本项目无三方运行依赖；测试用 JUnit5、基准用 JMH）。

```bash
# 全量测试（含 sp-size 零测试 + lcache-core 20 个用例）
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

## 6. 测试结果（本机：JDK 17.0.15，4 核）

`mvn -q -pl modules/lcache-core test` → **20 个用例全绿（0 失败 / 0 错误）**：

| 测试类 | 用例数 | 覆盖 |
|---|---|---|
| BasicCacheTest | 4 | put/get/remove/invalidate/putIfAbsent/containsKey/null 守卫/Builder 校验（两引擎） |
| ConcurrencyConsistencyTest | 3 | 多写线程独立键精确 size；同键并发写无丢失更新；并发读看不到非法值 |
| EvictionPolicyTest | 4 | FIFO/LRU 确定性淘汰、容量严格收口、更新不涨 size |
| ExpiryTest | 3 | 惰性过期 + 清扫回调 EXPIRED、未配置不过期、过期后重写刷新截止 |
| StatsTest | 3 | 命中/未命中、驱逐/过期计数、关闭统计返回空 |
| LoadingSingleFlightTest | 3 | 并发 get(K,loader) 只执行一次 loader、失败传播、null 拒绝 |

---

## 7. 基准结果（JMH，本机 4 核示例，`-t 4`）

### 引擎吞吐（ops/ms，越大越好）EngineThroughputBenchmark

| 基准 | LOCK_FREE | SYNCHRONIZED | 备注 |
|---|---|---|---|
| getHit | 23 475 | 3 166 | 读命中：无锁 ≈ **7.4×** |
| getMiss | 37 043 | 5 813 | 读未命中 |
| mixedReadWrite | 1 119 | 1 005 | 80/20 混合（写收口占主导） |
| putUpdate | 227 | 188 | 写路径两者都经 4 写线程收口 |

> 读路径无锁收益显著；写路径因"统一经线程亲和写池"两引擎相当，差距来自引擎内部 CAS vs 锁。

### size() 成本（ops/ms，`-t 8`）CacheSizeThroughputBenchmark

| 引擎 | ops/ms | 语义 |
|---|---|---|
| LOCK_FREE | 294 | SP per-thread 求和，**无锁、不阻塞任何写/读**，成本与数据量无关 |
| SYNCHRONIZED | 9 152 | `map.size()` O(1)，但要抢全局锁、与其它操作串行 |

> 取舍说明：全锁基线在"只有 size() 一个操作、完全隔离"时更快（O(1)）；无锁引擎的 size()
> 快在**不占用排它锁**，适合"读多写多还要频繁 size()"的真实混合负载。它的成本是 O(64 槽) 固定扫描，
> 与条目总量无关（对比普通遍历式 size 的 O(N)）。

### 后端场景 demo 摘要（DemoRunner，进程内请求线程负载）

- Phase 1 语义/写后失效：LRU 驱逐回调、读穿加载、失效后重载均正确，命中率 0.94；
- Phase 2 负载（8 客户端 × 15k，读多写少）：命中率 ≈0.79，精确 size≈4096；
- Phase 4 Zipf（300k 读，容量 256）：FIFO 命中率 96.85%，近似 LRU 98.87%
  （热点提升对长尾 Zipf 有效）。

---

## 8. 边界与使用条件（诚实清单）

1. **key 需可排序**：底层 SP 无锁表用有序链表组织桶 → key 需实现 `Comparable`，或 `builder.comparator(...)`；
   null 键/值被禁止。
2. **定容不可扩容**：`capacity` 在构建时确定（无锁哈希不可扩容）。语义层只承诺驱逐到 `maxSize`；
   若无需容量控制请给足 `capacity`。
3. **写线程 ≤ 64**：写路径收口到线程亲和池（`writerThreads` ∈ [1,64]），因为 SP per-thread 槽位上限 64。
4. **size() 含未清扫的过期条目**：过期条目在"判定 miss"与"清扫移除"之间仍被计数；`cleanUp()` 后精确。
5. **宽松一致性**：读与写并发时可能短暂读到旧值/过期值，按 miss 处理；不提供强线性化的 `getAndCompute` 之类。
6. **写为同步收口 + 过载有界**：`put/invalidate` 阻塞等待写池执行，提交经有界准入闸（`Semaphore(64)`）；
   极端过载（写线程长时间无法腾出闸位）时**快速失败抛 `RejectedExecutionException`，该次写不生效**，
   调用方需自行决定重试/降级（机制见 §2"写池过载保护"）。适合"读多写少 + 数据量可控"的 L1 场景，
   不适合海量纯写或要求写端超低延迟的极端场景。
7. **无持久化 / 崩溃不恢复 / 无网络层**：本缓存是**纯内存进程内组件**，不做 KV 落盘与多活。
8. **裁剪来源**：无锁哈希来自 SP 开源实现（见 `modules/sp-size/VENDOR.md`）；上层缓存语义与
   线程收口为本项目自研。

---


## 9. 上游与许可

- 无锁哈希/SP 计数子集来源见 `modules/sp-size/VENDOR.md`（GPL v3 头保留在各文件）。
- 本模块 `lcache-core`（缓存语义、并发收口、驱逐/TTL/读穿/统计）为原创实现，Apache-2.0 语义的开源可自行选用。
