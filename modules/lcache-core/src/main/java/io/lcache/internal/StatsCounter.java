package io.lcache.internal;

import io.lcache.CacheStats;

import java.util.concurrent.atomic.LongAdder;

/**
 * 统计计数器：全部用 JDK {@link LongAdder}（无锁、低竞争、线程安全），
 * {@link #snapshot()} 取不可变快照。开启统计的开关由缓存层负责（关闭时各方法直接返回）。
 */
final class StatsCounter {

    private final boolean enabled;
    private final LongAdder requests = new LongAdder();
    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();
    private final LongAdder evictions = new LongAdder();
    private final LongAdder expires = new LongAdder();
    private final LongAdder loads = new LongAdder();
    private final LongAdder loadFailures = new LongAdder();
    private final LongAdder loadNanos = new LongAdder();

    StatsCounter(boolean enabled) {
        this.enabled = enabled;
    }

    boolean enabled() {
        return enabled;
    }

    void recordRequest() {
        if (enabled) requests.increment();
    }

    void recordHit() {
        if (enabled) hits.increment();
    }

    void recordMiss() {
        if (enabled) misses.increment();
    }

    void recordEviction() {
        if (enabled) evictions.increment();
    }

    void recordExpiration() {
        if (enabled) expires.increment();
    }

    void recordLoadSuccess(long elapsedNanos) {
        if (enabled) {
            loads.increment();
            loadNanos.add(elapsedNanos);
        }
    }

    void recordLoadFailure(long elapsedNanos) {
        if (enabled) {
            loadFailures.increment();
            loadNanos.add(elapsedNanos);
        }
    }

    CacheStats snapshot() {
        if (!enabled) {
            return CacheStats.EMPTY;
        }
        return new CacheStats(requests.sum(), hits.sum(), misses.sum(),
                evictions.sum(), expires.sum(),
                loads.sum(), loadFailures.sum(), loadNanos.sum());
    }
}
