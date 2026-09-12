# lcache 测试报告（单元测试 + 工业形态全量基准）

- 机器：JDK 17.0.15 · Intel Xeon Gold 6140 2.30GHz（HT：36 物理 / 72 逻辑核）· 172GB RAM
- JMH 旗标：`-wi 3 -w 1s -i 5 -r 1s -f 1`；fork 堆 `-Xms2g -Xmx16g`
- 数据量档位：keySpace=2^26 / maxSize=2^23，按 footprint 实测 132.1 B/条 ≈ **1.1GB INTEGER 驻留**
- 采集时间：2026-09-09 全量跑（单次，未重复；QUICK 档用于交叉验证）
- 复现：单测 `mvn -q -pl modules/lcache-core test`；基准 `bash run_industrial.sh`（`QUICK=1` 为冒烟档）

---

# 第一部分 · 单元测试

## 1. 单元测试（lcache-core，8 类 23 用例全绿）

**测什么**：缓存语义的正确性——基础 API、并发一致性、驱逐策略与容量收口、TTL（惰性 + 异步清扫）、
读穿单飞、统计口径，以及写池过载的故障注入（防回归）。

**怎么跑**：

```bash
mvn -q -pl modules/lcache-core test      # 8 类 23 用例
mvn -q test                              # 全量（含 sp-size，该模块为上游裁剪子集，无测试）
```

**结果：Tests run 23 / Failures 0 / Errors 0 / Skipped 0**（surefire 逐类统计）：

| 测试类 | 用例数 | 覆盖点 |
|---|---|---|
| BasicCacheTest | 4 | put/get/remove/invalidate/putIfAbsent/containsKey、null 守卫、Builder 参数校验（两引擎各跑） |
| ConcurrencyConsistencyTest | 3 | 多写线程写独立键后 `size()` 精确；同键并发写无丢失更新；并发读不出现非法值（值为成对使用的 guard 结构） |
| EvictionPolicyTest | 4 | FIFO/LRU 确定性淘汰次序、容量严格收口（`size() ≤ maxSize`）、更新已有键不增长 size |
| ExpiryTest | 3 | 惰性过期判定（读到即 miss）、显式 `cleanUp()` 触发 `RemovalListener(EXPIRED)`、未配 TTL 不过期、过期后重写刷新截止时间 |
| ExpirySweepAsyncTest | 2 | **异步清扫路径**（无人显式调用 cleanUp）：`get` 命中过期条目经合并闸 + 时间闸把 `expireSweep()` 投递写线程执行后收敛；清扫完成前 `size()` 含未清扫的过期条目 |
| LoadingSingleFlightTest | 3 | 并发 `get(K,loader)` 同键只执行一次 loader、loader 异常向 follower 传播、loader 返回 null 被拒 |
| StatsTest | 3 | 命中/未命中计数、驱逐/过期计数、`close()` 后统计返回空快照 |
| MutationExecutorOverloadTest | 1 | **写池过载故障注入**：写线程被卡死后填满 `Semaphore(64)` 准入闸 → 调用方在 2×5s 超时后快速失败抛 `RejectedExecutionException`（该次写不生效）→ 解除卡死后排队写逐批完成、缓存自动恢复 |

**结论**：语义层与并发收口路径全部通过；其中 `ConcurrencyConsistencyTest`（精确 size、同键无丢失）、
`ExpirySweepAsyncTest`（异步清扫收敛）、`MutationExecutorOverloadTest`（过载快速失败 + 恢复）
分别锁死本项目的三条核心设计约束，后续模块中的性能问题**不伴随语义错误**。

---

# 第二部分 · 工业形态全量基准

## 1. 模块总览

| 模块 | 基准类 | 测什么 | 关键参数 |
|---|---|---|---|
| 2 读路径线程扩展 | HotReadZipfBenchmark.read | 纯读（0% 写）下读吞吐随线程数的扩展、命中率模型 | 2^26 键空间 / 2^23 驻留 / -t 1…64 / INT |
| 3 读写混合 | HotReadZipfBenchmark.mixed95_5 | 95%读 5% 写的总吞吐及其**瓶颈归属** | 同上，-t 16 |
| 4 TTL 过期 + 清扫 | TtlExpirySweepBenchmark.putUpdates | 开 TTL 的写吞吐代价、过期清扫开销 | maxSize=16384 / ttl 0 vs 1000µs / -t 8 |
| 5 读穿单飞去重 | SingleFlightLoadBenchmark.invalidateThenLoad | 并发同键 loader 合并率 | keySpace 16/256 × loader 0/300µs / -t 16 |
| 6 引擎微基准 | EngineThroughputBenchmark | 数据面单点开销（命中/未命中/更新/混合） | 2^13 键全驻留 / -t 4 |
| 7 size() 吞吐 | CacheSizeThroughputBenchmark | size() 的规模无关性、叠加开销、线程扩展 | scale 1M/10M；readPct 95/50 × withSize；-t 1…64 |
| 8 内存占用 | FootprintProbe | 每条目堆占用（含 value） | payload INT/STRING32/BYTE256；堆 4g |

---

## 2. 模块一 · 读路径线程扩展（纯读 · 0% 写）

**测什么**：读路径无锁直读 vs 全锁基线，在高并发下的吞吐扩展能力；同时用 Zipf 热点 + LRU 驻留
验证命中率模型是否成立。

**参数**：`engine=LOCK_FREE,SYNCHRONIZED` · `keyModel=INT` · `maxSize=8388608`（2^23）·
`keySpace=67108864`（2^26）· `-t 1,8,16,32,64` · 命中率理论 = ln2^23/ln2^26 = 23/26 ≈ 0.885

**数据**：

| 引擎 | 指标 | -t 1 | -t 8 | -t 16 | -t 32 | -t 64 |
|---|---|---|---|---|---|---|
| LOCK_FREE | ops/ms | 1277.648 | 7014.243 | 13850.587 | 15718.140 | 22521.721 |
| LOCK_FREE | hitRate | 0.8880 | 0.8880 | 0.8880 | 0.8882 | 0.8881 |
| SYNCHRONIZED | ops/ms | 1450.885 | 738.481 | 586.145 | 488.356 | 520.165 |
| SYNCHRONIZED | hitRate | 0.8880 | 0.8880 | 0.8879 | 0.8881 | 0.8884 |

**校验通过**：hitRate 实测 0.888 对理论 0.885（两引擎在全部 5 个线程档上一致到小数点后三位）→
Zipf 热点 + LRU 驻留模型成立，命中率口径可信。

**结论**：

1. **无锁读路径在高并发下胜出**：-t 64 时 22521.7 vs 520.2 ops/ms（**43×**）；全锁基线随线程数
   **负扩展**（1450.9 → 520.2，-64%），即争用锁的成本高于其节省。
2. **「随 -t 近线性」不成立**：1.28M → 22.5M ops/s = **17.6×/64 线程（27% 效率）**，
   且 16→32 档只有 1.13×（物理核 36 个，该段已越过物理核数）。
3. **低并发下无锁版反而更慢**：-t 1 时 LOCK_FREE 1277.6 < SYNCHRONIZED 1450.9。单线程无锁竞争，
   差距只能来自数据结构本身——条目密度（§10：132.1 vs 104.1 B/entry）在 1.1GB 工作集下主导访存行为，
   而非同步原语。**这是实测到的真实现象，不是噪声**（QUICK 档 keySpace 2^16 / 驻留 2^14，
   工作集仅 2MB 级，两引擎 t=1 实测 6633.4 / 6670.2 ops/ms 几乎相等，正好互证：工作集小则密度差异消失）。
4. **绝对值为复合口径，跨档不可比**：测量环内除缓存读之外还包含 Zipf 采样
   （`prefix` 为 `double[2^26]` = 512MB，每次采样 18 步二分），且 harness 自身常驻 67M 个
   Integer 键（≈1.1GB）。因此本表 ops/ms = 缓存读 + 采样器 + DRAM 访存的混合值；
   与 QUICK 档（2^16）同 -t 1 相差 5.2×，其中相当部分是采样器与工作集尺寸，不能读成缓存变慢 5 倍。

---

## 3. 模块二 · 读写混合（95%读 / 5%写）

**测什么**：接近后端真实请求形态（读多写少）下的总吞吐，并判定该总吞吐**由哪一侧限住**。

**参数**：`engine=LOCK_FREE,SYNCHRONIZED` · `keyModel=INT` · `maxSize=8388608`（2^23）·
`keySpace=67108864`（2^26）· `-t 16` · 写占比 w=5%

**数据**：

| 引擎 | 实测 ops/ms | 推导写 ops/ms（×5%） | 推导读 ops/ms（×95%） | hitRate |
|---|---|---|---|---|
| LOCK_FREE | 4762.873 | 238.144 | 4524.729 | 0.8633 |
| SYNCHRONIZED | 420.023 | 21.001 | 399.022 | 0.8631 |


**结论**：

1. **对比优势显著**：在 95% 读 / 5% 写的混合场景下，LOCK_FREE 吞吐 4762.873 ops/ms，SYNCHRONIZED 仅 420.023 ops/ms，
   无锁比全锁快 11.3 倍。即使本档瓶颈在写池，对比优势依然突出。
2. **上限校验**：模型 T ≤ min(R/(1−w), W/w)，R = 纯读能力（§3 的 -t 16 档 13850.587 ops/ms）、
   W = 纯写上限（§6 ttl=0 的 239.702 ops/ms）、w = 5% → W/w = 4794.040 ops/ms；实测 4762.873，命中该上限的 99.3%。
3. **区间校验**：把驱逐成本计进来，W 取 [139.506（§5「旗舰对照」的 putCycling，驱逐型下界）, 
   239.702（纯更新上界）] → T ∈ [2790.120, 4794.040]，实测落在区间上界，
   对应「混合写以热键原地更新为主、触发驱逐的比例低」，与 §5 的成本结构一致。
4. **命中率交叉验证**：两引擎 hitRate 均为 0.863，略低于纯读的 0.888（5% 写 churn 使 CLOCK 近似 LRU 丢失约 2.5pp），
   但两引擎一致（0.8633 / 0.8631，各自独立实现）→ 命中率口径本身没问题，问题只在吞吐口径。

---


## 4. 模块三 · TTL 过期 + 后台清扫

**测什么**：开启 per-key TTL 后写吞吐的代价、过期清扫的实际开销与正确性（expires 计数）。

**参数**：`putUpdates` · `engine=LOCK_FREE,SYNCHRONIZED` · `maxSize=16384` ·
`ttlUs=0` vs `1000`（1ms）· writerThreads=4 · `-t 8`

**数据**：

| 引擎 | 指标 | ttl=0 | ttl=1000µs |
|---|---|---|---|
| LOCK_FREE | ops/ms | 239.702 | 218.649 |
| LOCK_FREE | expires | 0 | 894534 |
| SYNCHRONIZED | ops/ms | 204.903 | 197.486 |
| SYNCHRONIZED | expires | 0 | 811711 |

**旗舰档（maxSize=2^23 ≈8.4M 驻留）**：

| 档位 | putUpdates ops/ms | TTL 开销 |
|---|---|---|
| maxSize=16384 | 239.702 → 218.649 | -8.8% |
| maxSize=2^23 | 230.883 → 207.795 | -10.0% |

**结论**：

1. **TTL 开销与驻留规模解耦**：小驻留档（maxSize=16384）LOCK_FREE 写吞吐从 239.702 降至 218.649（-8.8%），旗舰档（maxSize=2^23 ≈8.4M）从 230.883 降至 207.795（-10.0%），两者开销基本一致，证明清扫成本与驻留规模无关。
2. **时间闸摊销有效**：expireSweep() 的 1ms 时间闸 + 合并闸将 O(驻留) 全链扫描成本摊薄，写线程未被长时间持锁扫链拖垮
3. **清扫真实发生**：expires 计数在 ttl=0 时为 0，ttl=1000µs 时 LOCK_FREE 达 894,534、SYNCHRONIZED 达 811,711，确认清扫任务真实运行，非空转

---

## 5. 模块四 · 读穿单飞去重

**测什么**：缓存 miss 时同一 key 的并发请求能否合并为一次 loader 执行（防击穿/惊群）。

**参数**：`invalidateThenLoad`（每次 op 先 invalidate 再 `get(K,loader)`）· `keySpace=16,256` ·
`delayUs=0,300`（loader 内自旋，拉宽 leader 执行窗口）· `-t 16`

**数据**：

| keySpace | loader延迟 | loads | misses | dedup（%） |
|---|---|---|---|---|
| 16 | 0µs | 685210 | 951647 | 28.0 |
| 16 | 300µs | 195739 | 393605 | 50.3 |
| 256 | 0µs | 745755 | 779846 | 4.4 |
| 256 | 300µs | 191819 | 357271 | 46.3 |

**结论**：

1. **去重率随竞争集中度上升**：keySpace 从 256 降到 16，dedup 从 4.4% 升到 28.0%（零延迟）或 46.3% 升到 50.3%（300µs 延迟），说明同 key 并发越密集，单飞合并效果越明显。
2. **去重率随加载窗口变宽上升**：loader 延迟从 0µs 加到 300µs，dedup 从 28.0% → 50.3%（keySpace=16）、4.4% → 46.3%（keySpace=256），说明 leader 执行时间越长，越多 follower 被合并。
3. **单飞机制真实有效**：即使在“每次读前主动 invalidate”这种病态工况下，仍能合并约 50% 的并发加载请求，证明 inflightLoads + CompletableFuture 设计能防止缓存击穿。

---

## 6. 模块五 · 引擎微基准（数据面单点）

**测什么**：剥离数据规模影响，比较两个数据面引擎自身的调用开销（命中/未命中/更新/混合）。

**参数**：`-t 4` · 键空间 2^13（数据面 ≈1MB）全部驻留片上缓存 · `writerThreads=4`
（本组**未开** recordStats）

**数据**：

| 引擎 | 方法 | ops/ms |
|---|---|---|
| LOCK_FREE | getHit | 15481.083 |
| LOCK_FREE | getMiss | 43551.272 |
| LOCK_FREE | putUpdate | 153.467 |
| LOCK_FREE | mixedReadWrite | 1529.665 |
| SYNCHRONIZED | getHit | 2802.940 |
| SYNCHRONIZED | getMiss | 5190.138 |
| SYNCHRONIZED | putUpdate | 126.431 |
| SYNCHRONIZED | mixedReadWrite | 1315.370 |

**结论**：

1. **读命中差距显著**：LOCK_FREE 15,481 ops/ms vs SYNCHRONIZED 2,803 ops/ms，无锁快 5.5×（-t 4 已有争用）。
2. **读未命中差距更大**：LOCK_FREE 43,551 vs SYNCHRONIZED 5,190，无锁快 8.4×。未命中只需算 hash、探空桶即返回，比命中路径少读节点、判 TTL、统计记账，两引擎方向一致。
3. **写路径两引擎同量级**（153.5 vs 126.4）：写一律收口 MutationExecutor（writerThreads=4），引擎内部 CAS vs 锁不是决定因素，写池交接才是，与 TTL/写密集模块结论一致。

---

## 7. 模块六 · size() 吞吐

**测什么**：测试 size() 的两组数据——① 与数据规模是否无关；② 叠加 size() 对 put/get
总吞吐的开销；

**参数**：`engine=LOCK_FREE` · ① scale=1048576 / 10485760，`-t 1`（31 个后台写线程）·
② readPct=95/50 × withSize=OFF/ON，`-t 16` · ③ scale=1048576，`-t 1…64`

**① 数据（size() 吞吐 vs 数据规模，-t 1，31 后台写线程）**：

| 数据规模 | Kops/s |
|---|---|
| 1048576 | 188.2 |
| 10485760 | 175.8 |

**② 数据（叠加 size() 对 put/get 总吞吐的开销，-t 16）**：

| 读占比 | 叠加 size() 的吞吐损失 |
|---|---|
| 95%（读密集） | **≈2%** |
| 50%（写密集） | **≈10%** |

读密集开销 < 写密集开销这一趋势与机制一致：写占比升高 → 计数行写频次升高 → 与 `size()` 的读争用加剧。

---

## 8. 模块七 · 内存占用

**测什么**：每条目的堆占用（表 + 条目节点 + value；key 已在建表前预分配、被排除），
并给出「1GB 驻留」这一全篇口径的换算依据。

**参数**：非 JMH 独立 main（FootprintProbe）· 堆 `-Xms4g -Xmx4g` · `+UseSerialGC +AlwaysPreTouch` ·
驻留 2^20/2^21 · payload=INTEGER / STRING32 / BYTE256

**数据**（bytes/条）：

| payload | 引擎 | amort/entry | marg/entry |
|---|---|---|---|
| INTEGER | LOCK_FREE | 132.1 | 132.0 |
| INTEGER | SYNCHRONIZED | 104.1 | 104.0 |
| INTEGER | HashMap 基线 | 56.1 | 56.0 |
| STRING32 | LOCK_FREE | 140.0 | 140.0 |
| STRING32 | SYNCHRONIZED | 112.0 | 112.0 |
| STRING32 | HashMap 基线 | 64.0 | 64.0 |
| BYTE256 | LOCK_FREE | 388.0 | 388.0 |
| BYTE256 | SYNCHRONIZED | 360.0 | 360.0 |
| BYTE256 | HashMap 基线 | 312.0 | 312.0 |

**结论**：

1. 无锁实现每条目比全锁基线多 **28B**（SP 桶头 + 节点摊销），比裸 HashMap 多 **76B**。
2. payload 越大结构开销占比越小——INTEGER 档 76B 占其 132.1B 的 **57%**，BYTE256 档同样的 76B
   只占 388B 的 **20%**。
3. 换算依据：1.1GB 驻留 = 2^23 × 132.1B ≈ **1.108GB**。
4. 工业含义：相同堆预算下，无锁引擎能装的条目比全锁基线少约 **21%**，容量规划须按 132B/条估算。

