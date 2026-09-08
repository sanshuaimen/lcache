package io.lcache;

/**
 * 缓存统计的不可变快照。
 * <p>
 * 指标语义（后端可观测性语言）：
 * <ul>
 *   <li>{@code requestCount} 访问总次数；</li>
 *   <li>{@code hitCount / missCount} 命中/未命中次数；</li>
 *   <li>{@code evictionCount / expireCount} 因容量驱逐/TTL 到期的移除次数；</li>
 *   <li>{@code loadCount / loadFailureCount} 读穿加载发起/失败次数；</li>
 *   <li>{@code totalLoadTimeNanos} 加载总耗时（用于平均加载延迟）。</li>
 * </ul>
 * 命中率 = hit / request，用于后端"缓存是否有效"的粗判。
 */
public final class CacheStats {

    /** 空统计（未开启统计或尚未发生访问）。 */
    public static final CacheStats EMPTY = new CacheStats(0, 0, 0, 0, 0, 0, 0, 0);

    private final long requestCount;
    private final long hitCount;
    private final long missCount;
    private final long evictionCount;
    private final long expireCount;
    private final long loadCount;
    private final long loadFailureCount;
    private final long totalLoadTimeNanos;

    /**
     * 由统计模块在内存汇总后构造快照。
     */
    public CacheStats(long requestCount, long hitCount, long missCount,
                      long evictionCount, long expireCount,
                      long loadCount, long loadFailureCount, long totalLoadTimeNanos) {
        this.requestCount = requestCount;
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.evictionCount = evictionCount;
        this.expireCount = expireCount;
        this.loadCount = loadCount;
        this.loadFailureCount = loadFailureCount;
        this.totalLoadTimeNanos = totalLoadTimeNanos;
    }

    public long requestCount() { return requestCount; }
    public long hitCount() { return hitCount; }
    public long missCount() { return missCount; }
    public long evictionCount() { return evictionCount; }
    public long expireCount() { return expireCount; }
    public long loadCount() { return loadCount; }
    public long loadFailureCount() { return loadFailureCount; }
    public long totalLoadTimeNanos() { return totalLoadTimeNanos; }

    /** 命中率 [0,1]；无访问时为 0。 */
    public double hitRate() {
        return requestCount == 0 ? 0.0d : (double) hitCount / requestCount;
    }

    /** 未命中率。 */
    public double missRate() {
        return 1.0d - hitRate();
    }

    /** 平均读穿加载耗时（纳秒）；无加载时为 0。 */
    public double averageLoadPenaltyNanos() {
        return loadCount == 0 ? 0.0d : (double) totalLoadTimeNanos / loadCount;
    }

    @Override
    public String toString() {
        return "CacheStats{" +
                "requests=" + requestCount +
                ", hits=" + hitCount +
                ", misses=" + missCount +
                ", hitRate=" + String.format("%.4f", hitRate()) +
                ", evictions=" + evictionCount +
                ", expires=" + expireCount +
                ", loads=" + loadCount +
                ", loadFailures=" + loadFailureCount +
                ", avgLoadNanos=" + String.format("%.1f", averageLoadPenaltyNanos()) +
                '}';
    }
}
