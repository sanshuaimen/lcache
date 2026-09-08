package io.lcache.internal;

import io.lcache.CacheBuilder;
import io.lcache.CacheStats;
import io.lcache.Clock;
import io.lcache.EngineKind;
import io.lcache.LocalCache;
import io.lcache.PolicyKind;
import io.lcache.RemovalCause;
import io.lcache.RemovalListener;
import io.lcache.engine.CacheEngine;
import io.lcache.engine.LockFreeCacheEngine;
import io.lcache.engine.SynchronizedCacheEngine;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * 本地缓存语义实现——本项目的核心交付物。
 *
 * <h2>并发模型总览（对应无锁/JMM 主线）</h2>
 * <pre>
 *   读路径 get/containsKey        —— 任意线程，无锁直读引擎哈希表（锁无关）
 *   写路径 put/invalidate/putIfAbsent —— 收口到线程亲和写池（MutationExecutor），
 *        池内写线程已注册 SP 计数槽；调用方阻塞等待结果
 *   顺序/元数据（驱逐链表 + 驱逐点 CLOCK 二次机会 + 时间闸过期清扫）—— 写线程在 orderLock 下维护
 *   数据面引擎                     —— SP 无锁哈希表（默认）或全锁表（基线）
 * </pre>
 *
 * <b>为什么读快写收口</b>：SP 无锁引擎对读是纯 volatile 读；对写要求"当前线程已注册
 * per-thread 计数槽（≤64）"。业务线程不可控且可能超 64，故写路径一律汇入固定写线程池，
 * 读路径保持无锁直读 → 读吞吐不被写线程数约束，写正确性由固定槽位保证。
 *
 * <b>为什么驱逐顺序只在写侧维护</b>：若每次读都去移动 LRU 链表，读路径就要写共享内存，
 * 与"读无锁"冲突。因此读路径只对一个节点做原子自增（读计数），不触碰链表；提升推迟到
 * <b>驱逐决策点</b>才兑现：驱逐时对"近期被读"的队头做<b>有界 CLOCK 二次机会</b>（复位计数、
 * 转队尾继续探测，常数上限 {@code CLOCK_PROBES}）——写成本 O(1) 且与驻留集规模无关，
 * 免去了周期整链扫描。显式 {@link #cleanUp()} 仍做整链提升（完整维护语义）。
 *
 * <b>一致性口径（宽松）</b>：读可能短暂观察到：已驱逐但读早已握住的旧节点、或判定过期但
 * 尚未被清扫的条目——按后端缓存惯例，读到过期值视为 miss；清扫由维护任务异步收敛。
 */
public final class LocalCacheImpl<K, V> implements LocalCache<K, V> {

    // ── 组件 ─────────────────────────────────────────────────────

    /** 数据面引擎。 */
    private final CacheEngine<K, CacheNode<K, V>> engine;
    /** 可注入时钟（TTL 判断）。 */
    private final Clock clock;
    /** per-entry TTL（纳秒），0 = 不过期。 */
    private final long ttlNanos;
    /** 容量上限，0 = 不驱逐。 */
    private final long maxSize;
    /** 驱逐策略。 */
    private final PolicyKind policy;
    private final boolean recordStats;
    private final RemovalListener<K, V> removalListener;

    /** 线程亲和写池（写/维护路径）。 */
    private final MutationExecutor mutator;
    /** 统计。 */
    private final StatsCounter stats;
    /** 读穿单飞去重表：key → 进行中的加载。 */
    private final ConcurrentHashMap<K, CompletableFuture<V>> inflightLoads = new ConcurrentHashMap<>();

    // ── 顺序链表（仅 orderLock 下访问） ──
    private final ReentrantLock orderLock = new ReentrantLock();
    private CacheNode<K, V> head;
    private CacheNode<K, V> tail;

    /** 过期清扫合并闸：避免频繁触发时堆积大量清扫任务。 */
    private final AtomicInteger drainGate = new AtomicInteger(0);

    /** CLOCK 二次机会：驱逐时最多给"近期被读的队头"救活转队尾的次数（常数，写成本与驻留集无关）。 */
    private static final int CLOCK_PROBES = 8;
    /** 过期清扫时间闸：距上次实际清扫不足该间隔的触发视为 no-op，摊销全链 O(n) 扫描。 */
    private final long expirySweepIntervalNanos;
    /** 上次实际执行过期清扫的时刻（volatile；写线程更新）。 */
    private volatile long lastExpirySweepNanos;

    private boolean hasExpiry() {
        return ttlNanos > 0L;
    }

    private boolean promoteReads() {
        return maxSize > 0L && policy == PolicyKind.LRU;
    }

    /** 过期清扫间隔：按 ttl/8 派生并夹在 [1ms, 1s]，保证摊销又不至于清扫过稀。 */
    private static long resolveExpirySweepInterval(long ttlNanos) {
        if (ttlNanos <= 0L) {
            return Long.MAX_VALUE; // 无过期：永不触发
        }
        long base = ttlNanos / 8;
        long min = TimeUnit.MILLISECONDS.toNanos(1);
        long max = TimeUnit.SECONDS.toNanos(1);
        return Math.min(Math.max(base, min), max);
    }

    // ── 构造 ─────────────────────────────────────────────────────

    /**
     * 由 {@link CacheBuilder} 调用。公开仅为跨包构造；不建议业务直接使用。
     */
    public LocalCacheImpl(int capacity, EngineKind engineKind, long maxSize, PolicyKind policy,
                   long ttlNanos, Comparator<? super K> comparator, Clock clock,
                   int writerThreads, RemovalListener<K, V> listener, boolean recordStats) {
        this.maxSize = maxSize;
        this.policy = policy;
        this.ttlNanos = ttlNanos;
        this.clock = Objects.requireNonNull(clock);
        this.recordStats = recordStats;
        this.removalListener = Objects.requireNonNull(listener);
        this.mutator = new MutationExecutor(writerThreads);
        this.stats = new StatsCounter(recordStats);
        this.engine = createEngine(engineKind, capacity, comparator);
        this.expirySweepIntervalNanos = resolveExpirySweepInterval(ttlNanos);
    }

    @SuppressWarnings("unchecked")
    private CacheEngine<K, CacheNode<K, V>> createEngine(EngineKind kind, int capacity,
                                                         Comparator<? super K> comparator) {
        if (kind == EngineKind.LOCK_FREE) {
            return new LockFreeCacheEngine<>(capacity, comparator);
        }
        if (kind == EngineKind.SYNCHRONIZED) {
            return new SynchronizedCacheEngine<>();
        }
        throw new IllegalArgumentException("unknown engine: " + kind);
    }

    // ── 读路径（无锁直读） ────────────────────────────────────────

    @Override
    public V get(K key) {
        Objects.requireNonNull(key);
        return getIfPresent(key);
    }

    @Override
    public V getIfPresent(K key) {
        Objects.requireNonNull(key);
        stats.recordRequest();
        // 无锁读：engine.get 对单条 value 的 volatile/acquire 读即线性化点
        CacheNode<K, V> node = engine.get(key);
        if (node == null) {
            stats.recordMiss();
            return null;
        }
        long now = clock.currentTimeNanos();
        if (node.isExpired(now)) {
            // 惰性过期：判定 miss，交由维护任务异步清扫（读路径不执行 SP 写）
            stats.recordMiss();
            maybeScheduleExpirySweep();
            return null;
        }
        stats.recordHit();
        if (promoteReads()) {
            // 读路径唯一的共享写：一个原子自增，作为 LRU 近似依据
            node.recordRead();
        }
        return node.value;
    }

    @Override
    public boolean containsKey(K key) {
        Objects.requireNonNull(key);
        CacheNode<K, V> node = engine.get(key);
        if (node == null) {
            return false;
        }
        if (node.isExpired(clock.currentTimeNanos())) {
            maybeScheduleExpirySweep();
            return false;
        }
        return true;
    }

    @Override
    public V get(K key, Function<? super K, ? extends V> loader) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(loader);
        V present = getIfPresent(key);
        if (present != null) {
            return present;
        }
        return loadSingleFlight(key, loader);
    }

    /**
     * 读穿单飞加载：同一 key 只允许一个线程执行 loader（防击穿/惊群）。
     * leader 在调用线程执行 loader（不占写池）；其余线程等待其 future。
     */
    private V loadSingleFlight(K key, Function<? super K, ? extends V> loader) {
        CompletableFuture<V> future = new CompletableFuture<>();
        CompletableFuture<V> prior = inflightLoads.putIfAbsent(key, future);
        if (prior != null) {
            // follower：等待 leader 结果
            try {
                return prior.get();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while waiting for load of key=" + key, ie);
            } catch (ExecutionException ee) {
                throw rethrowUnchecked(ee.getCause());
            }
        }
        long t0 = clock.currentTimeNanos();
        try {
            V loaded = loader.apply(key);
            if (loaded == null) {
                throw new NullPointerException("loader returned null for key=" + key);
            }
            put(key, loaded); // 收口写池
            stats.recordLoadSuccess(clock.currentTimeNanos() - t0);
            future.complete(loaded);
            return loaded;
        } catch (Throwable t) {
            stats.recordLoadFailure(clock.currentTimeNanos() - t0);
            future.completeExceptionally(t);
            throw rethrowUnchecked(t);
        } finally {
            inflightLoads.remove(key, future);
        }
    }

    // ── 写路径（收口到写池） ──────────────────────────────────────

    @Override
    public V put(K key, V value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value, "null value is not allowed");
        // 提交到写池；池线程已注册 SP 计数槽，因此引擎写路径合法
        return mutator.submitAndAwait(() -> put0(key, value));
    }

    @Override
    public V putIfAbsent(K key, V value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value, "null value is not allowed");
        return mutator.submitAndAwait(() -> putIfAbsent0(key, value));
    }

    @Override
    public V invalidate(K key) {
        Objects.requireNonNull(key);
        return mutator.submitAndAwait(() -> remove0(key, RemovalCause.EXPLICIT));
    }

    // ── 写/维护核心（必须在写池线程上执行） ────────────────────────

    private V put0(K key, V value) {
        long expireAt = hasExpiry() ? clock.currentTimeNanos() + ttlNanos : 0L;
        CacheNode<K, V> node = new CacheNode<>(key, value, expireAt);
        // 线性化点：底层无锁表 CAS 发布新节点
        CacheNode<K, V> prev = engine.put(key, node);

        orderLock.lock();
        try {
            if (prev != null) {
                // 替换旧条目：旧节点已不在引擎中，从顺序链表摘除
                unlink(prev);
            }
            linkLast(node);
        } finally {
            orderLock.unlock();
        }

        // 容量超限即时驱逐（此时新节点位于队尾，队头最老）
        if (maxSize > 0L) {
            evictExcess();
        }
        maybeScheduleExpirySweep();
        return prev == null ? null : prev.value;
    }

    private V putIfAbsent0(K key, V value) {
        long expireAt = hasExpiry() ? clock.currentTimeNanos() + ttlNanos : 0L;
        CacheNode<K, V> node = new CacheNode<>(key, value, expireAt);
        CacheNode<K, V> prev = engine.putIfAbsent(key, node);
        if (prev != null) {
            // 键已存在，node 未被发布，直接丢弃
            return prev.value;
        }
        orderLock.lock();
        try {
            linkLast(node);
        } finally {
            orderLock.unlock();
        }
        if (maxSize > 0L) {
            evictExcess();
        }
        maybeScheduleExpirySweep();
        return null;
    }

    /** 按 key 删除并返回旧值；若当前引擎节点与目标不同（被替换过）则跳过。 */
    private V remove0(K key, RemovalCause cause) {
        CacheNode<K, V> removed = engine.remove(key);
        if (removed == null) {
            return null;
        }
        orderLock.lock();
        try {
            unlink(removed);
        } finally {
            orderLock.unlock();
        }
        afterRemoval(removed, cause);
        return removed.value;
    }

    /**
     * 容量驱逐：在写线程上循环逐出直到不超过 maxSize。
     *
     * <p><b>CLOCK 二次机会</b>（LRU 提升在此完成，替代周期性整链扫描）：队头若近期被读
     * （读计数 &gt; 0），给它一次"复活"——复位读计数并转到队尾，继续探测更老节点；
     * 探测上限 {@link #CLOCK_PROBES} 为常数 → 单次驱逐最坏 O(常数)，写成本与驻留集规模无关。
     * 读路径零改动（只做一次原子自增）；提升只发生在驱逐决策点——唯一需要新鲜次序的地方。
     * FIFO 不记读计数，探测立即判未读 → 退化为纯队头淘汰，行为不变。
     *
     * <p>驱逐用 removeIfValue 防止误删被并发替换出的新节点（身份校验）。与读者
     * {@code recordRead} 的竞态属于宽松 LRU 近似口径（见类注释），可忽略。
     */
    private void evictExcess() {
        while (engine.size() > maxSize) {
            CacheNode<K, V> victim;
            orderLock.lock();
            try {
                if (promoteReads()) {
                    int probes = 0;
                    while (probes < CLOCK_PROBES) {
                        CacheNode<K, V> h = head;
                        if (h == null) {
                            return; // 链空（引擎已无条目）
                        }
                        if (h.getAndResetReads() > 0) { // 近期被读 → 二次机会，转队尾并复位
                            unlink(h);
                            linkLast(h);
                            probes++;
                        } else {
                            break; // 队头未被读 → 即为受害者
                        }
                    }
                    // probes 耗尽仍未找到未读队头 → 按上限驱逐当前队头（容量压力下的正常退化）
                }
                victim = head;
                if (victim == null) {
                    return;
                }
                unlink(victim);
            } finally {
                orderLock.unlock();
            }
            CacheNode<K, V> removed = engine.removeIfValue(victim.key, victim);
            if (removed != null) {
                afterRemoval(removed, RemovalCause.EVICTED);
            }
            // removed == null：该节点已被并发替换/删除，跳过（新节点保留）
        }
    }

    /** 顺序链表队尾追加（须持 orderLock）。 */
    private void linkLast(CacheNode<K, V> node) {
        node.linked = true;
        if (tail == null) {
            head = tail = node;
        } else {
            tail.next = node;
            node.prev = tail;
            tail = node;
        }
    }

    /** 从顺序链表摘除（须持 orderLock，幂等）。 */
    private void unlink(CacheNode<K, V> node) {
        if (!node.linked) {
            return;
        }
        node.linked = false;
        CacheNode<K, V> p = node.prev;
        CacheNode<K, V> s = node.next;
        if (p != null) {
            p.next = s;
        } else {
            head = s;
        }
        if (s != null) {
            s.prev = p;
        } else {
            tail = p;
        }
        node.prev = node.next = null;
    }

    /**
     * 移除后的收尾：统计 + 触发监听器。
     * 在写线程执行；监听器异常不打断主流程。
     */
    private void afterRemoval(CacheNode<K, V> node, RemovalCause cause) {
        if (cause == RemovalCause.EVICTED) {
            stats.recordEviction();
        } else if (cause == RemovalCause.EXPIRED) {
            stats.recordExpiration();
        }
        try {
            removalListener.onRemoval(node.key, node.value, cause);
        } catch (Throwable t) {
            System.err.println("[lcache] removal listener threw for key=" + node.key + ": " + t);
        }
    }

    // ── 维护：过期清扫 + LRU 提升 + 容量收口 ───────────────────────

    /** 清扫过期节点：摘链后逐个用 removeIfValue 移除引擎（防误删新节点）。 */
    private void expireSweep() {
        if (!hasExpiry()) {
            return;
        }
        long now = clock.currentTimeNanos();
        lastExpirySweepNanos = now; // 记录实际清扫时刻，供时间闸摊销调度频率
        List<CacheNode<K, V>> expired = new ArrayList<>();
        orderLock.lock();
        try {
            CacheNode<K, V> c = head;
            while (c != null) {
                CacheNode<K, V> next = c.next;
                if (c.isExpired(now)) {
                    unlink(c);
                    expired.add(c);
                }
                c = next;
            }
        } finally {
            orderLock.unlock();
        }
        for (CacheNode<K, V> e : expired) {
            CacheNode<K, V> removed = engine.removeIfValue(e.key, e);
            if (removed != null) {
                afterRemoval(removed, RemovalCause.EXPIRED);
            }
            // 否则已被并发替换（同 key 已写入新值），保留新值
        }
    }

    /**
     * LRU 读计数整链提升：把近期被读（reads>0）的节点移到队尾并清零计数。
     * <p>只在<b>显式维护</b>（{@code cleanUp()}/runMaintenance）中调用；写路径的日常提升
     * 由驱逐点的 CLOCK 二次机会承担（见 {@link #evictExcess()}），不在此扫全链。
     */
    private void drainPromotions() {
        if (!promoteReads()) {
            return;
        }
        orderLock.lock();
        try {
            CacheNode<K, V> c = head;
            while (c != null) {
                CacheNode<K, V> next = c.next;
                if (c.getAndResetReads() > 0) {
                    unlink(c);
                    linkLast(c);
                }
                c = next;
            }
        } finally {
            orderLock.unlock();
        }
    }

    /** 显式完整维护（cleanUp 语义）：过期清扫 + 整链提升 + 容量收口，同步执行。 */
    private void runMaintenance() {
        expireSweep();
        drainPromotions();
        if (maxSize > 0L) {
            evictExcess();
        }
    }

    /**
     * 需要时异步触发一次<b>过期清扫</b>（合并闸 + 时间闸双防堆积）。
     *
     * <p>仅服务 TTL 场景。LRU 提升不再由此调度——改在驱逐点做 CLOCK 二次机会
     * （见 {@link #evictExcess()}），故纯 LRU 无 TTL 时写路径<b>零调度、零扫描</b>。
     * 时间闸：距上次实际清扫不足 {@link #expirySweepIntervalNanos} 的触发直接 no-op，
     * 把偶发的 O(驻留集) 全链扫描摊销到间隔之上；{@code cleanUp()} 走
     * {@link #runMaintenance()} 仍是同步强制全维护，不受闸限。
     */
    private void maybeScheduleExpirySweep() {
        if (!hasExpiry()) {
            return;
        }
        long now = clock.currentTimeNanos();
        if (now - lastExpirySweepNanos < expirySweepIntervalNanos) {
            return;
        }
        if (drainGate.compareAndSet(0, 1)) {
            try {
                mutator.execute(() -> {
                    drainGate.set(0);
                    expireSweep();
                });
            } catch (RuntimeException e) {
                drainGate.set(0);
                throw e;
            }
        }
    }

    // ── 其它公开 API ─────────────────────────────────────────────

    @Override
    public int size() {
        // 无锁引擎：O(写线程数) 精确计数（含未清扫的过期条目，cleanUp 后精确）
        return engine.size();
    }

    @Override
    public CacheStats stats() {
        return stats.snapshot();
    }

    @Override
    public void cleanUp() {
        mutator.submitAndAwait(() -> {
            runMaintenance();
            return null;
        });
    }

    @Override
    public void close() {
        mutator.close();
    }

    private static RuntimeException rethrowUnchecked(Throwable t) {
        if (t instanceof RuntimeException re) {
            return re;
        }
        if (t instanceof Error er) {
            throw er; // Error 保持抛出（含 ThreadDeath/OOM 等），不做包装
        }
        return new RuntimeException(t);
    }
}
