package io.lcache;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;

/**
 * {@link LocalCache} 的配置器（Builder）。
 * <p>
 * 使用示例：
 * <pre>
 * LocalCache&lt;Integer, String&gt; cache = LocalCache.&lt;Integer, String&gt;builder()
 *     .capacity(1 &lt;&lt; 16)
 *     .maxSize(10_000)
 *     .expireAfterWrite(10, TimeUnit.SECONDS)
 *     .policy(PolicyKind.LRU)
 *     .engine(EngineKind.LOCK_FREE)
 *     .removalListener((k, v, cause) -&gt; { ... })
 *     .recordStats()
 *     .writerThreads(4)
 *     .build();
 * </pre>
 *
 * @param <K> 键类型（需可排序：Comparable 或通过 {@link #comparator(Comparator)} 提供，
 *            因为底层 SP 哈希表用有序链表组织桶）
 * @param <V> 值类型
 */
public final class CacheBuilder<K, V> {

    private EngineKind engine = EngineKind.LOCK_FREE;
    private int capacity = 0;                 // 0 表示按 maxSize 自动推导
    private long maxSize = 0;                 // 0 表示不设容量上限（不驱逐）
    private PolicyKind policy = PolicyKind.LRU;
    private boolean recordStats = false;
    private long ttlNanos = 0;                // 0 表示不过期
    private RemovalListener<K, V> removalListener = (k, v, cause) -> { };
    private Comparator<? super K> comparator = null;
    private Clock clock = Clock.system();
    private int writerThreads = Math.min(4, Runtime.getRuntime().availableProcessors());

    /** 底层哈希表桶数（SP 哈希表定容不可扩容）。 */
    public CacheBuilder<K, V> capacity(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        this.capacity = capacity;
        return this;
    }

    /** 缓存条目容量上限；超过后按驱逐策略淘汰。0 表示不设上限。 */
    public CacheBuilder<K, V> maxSize(long maxSize) {
        if (maxSize < 0) {
            throw new IllegalArgumentException("maxSize must be >= 0: " + maxSize);
        }
        this.maxSize = maxSize;
        return this;
    }

    /** 写入后存活时长（per-entry TTL）。0 禁用。 */
    public CacheBuilder<K, V> expireAfterWrite(long duration, TimeUnit unit) {
        this.ttlNanos = Clock.toNanos(duration, unit);
        return this;
    }

    /** 容量驱逐策略（默认 LRU）。 */
    public CacheBuilder<K, V> policy(PolicyKind policy) {
        this.policy = java.util.Objects.requireNonNull(policy);
        return this;
    }

    /** 数据面引擎（默认无锁）。 */
    public CacheBuilder<K, V> engine(EngineKind engine) {
        this.engine = java.util.Objects.requireNonNull(engine);
        return this;
    }

    /** 开启统计。 */
    public CacheBuilder<K, V> recordStats() {
        this.recordStats = true;
        return this;
    }

    /** 移除监听器（驱逐/过期/显式失效）。 */
    public CacheBuilder<K, V> removalListener(RemovalListener<? super K, ? super V> listener) {
        this.removalListener = (RemovalListener<K, V>) java.util.Objects.requireNonNull(listener);
        return this;
    }

    /** 键排序比较器；为 null 时要求键实现 {@link Comparable}。 */
    public CacheBuilder<K, V> comparator(Comparator<? super K> comparator) {
        this.comparator = comparator;
        return this;
    }

    /** 可注入时钟（测试用）。 */
    public CacheBuilder<K, V> clock(Clock clock) {
        this.clock = java.util.Objects.requireNonNull(clock);
        return this;
    }

    /**
     * 内部写线程池大小（1..64）。这些线程是唯一会执行"写/维护"路径的线程，
     * 各自注册一个稳定的 SP per-thread 计数槽，因此不能超过 {@code ThreadID.MAX_THREADS}=64。
     */
    public CacheBuilder<K, V> writerThreads(int writerThreads) {
        if (writerThreads < 1 || writerThreads > io.lcache.internal.ThreadSlots.MAX_SLOTS) {
            throw new IllegalArgumentException("writerThreads must be in [1, 64]: " + writerThreads);
        }
        this.writerThreads = writerThreads;
        return this;
    }

    /** 依据当前配置构建缓存实例。 */
    public LocalCache<K, V> build() {
        int effCapacity = resolveCapacity();
        EngineKind effEngine = engine;
        long effTtl = ttlNanos;
        // TTL 需要维护线程清扫过期条目
        return new io.lcache.internal.LocalCacheImpl<>(effCapacity, effEngine, maxSize, policy,
                effTtl, comparator, clock, writerThreads, removalListener, recordStats);
    }

    private int resolveCapacity() {
        if (capacity > 0) {
            return capacity;
        }
        long base = Math.max(1024, maxSize);
        return (int) Long.highestOneBit(base) < base ? (int) (Long.highestOneBit(base) << 1) : (int) base;
    }
}
