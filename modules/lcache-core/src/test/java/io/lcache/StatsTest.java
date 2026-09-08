package io.lcache;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 统计测试：命中/未命中/驱逐/过期计数。
 */
class StatsTest {

    @Test
    void hitAndMissCounted() {
        try (LocalCache<Integer, String> c = LocalCache.<Integer, String>builder()
                .capacity(1024)
                .recordStats()
                .writerThreads(1)
                .build()) {
            c.put(1, "a");
            assertEquals("a", c.getIfPresent(1)); // hit
            assertTrue(c.getIfPresent(2) == null); // miss
            c.getIfPresent(1); // hit

            CacheStats s = c.stats();
            assertEquals(3, s.requestCount());
            assertEquals(2, s.hitCount());
            assertEquals(1, s.missCount());
            assertTrue(s.hitRate() > 0.66d);
        }
    }

    @Test
    void evictionAndExpiryCounted() {
        TestClock clock = new TestClock();
        try (LocalCache<Integer, String> c = LocalCache.<Integer, String>builder()
                .capacity(1024)
                .maxSize(2)
                .policy(PolicyKind.FIFO)
                .expireAfterWrite(10_000, TimeUnit.NANOSECONDS)
                .clock(clock)
                .recordStats()
                .writerThreads(1)
                .build()) {
            c.put(1, "a");
            c.put(2, "b");
            c.put(3, "c"); // 驱逐 1
            c.cleanUp();

            c.put(4, "d"); // 驱逐 2
            c.put(5, "e"); // 驱逐 3
            c.put(6, "f"); // 驱逐 4（5 条目顺序 2,3,4,5,6 → 超过则淘汰队头）
            clock.advance(20_000);
            c.cleanUp(); // 全部过期清扫

            CacheStats s = c.stats();
            assertTrue(s.evictionCount() >= 2, "evictions=" + s.evictionCount());
            assertTrue(s.expireCount() >= 1, "expires=" + s.expireCount());
            assertEquals(0, c.size());
        }
    }

    @Test
    void statsDisabled_returnsEmpty() {
        try (LocalCache<Integer, String> c = LocalCache.<Integer, String>builder()
                .capacity(1024)
                .writerThreads(1)
                .build()) {
            c.put(1, "a");
            c.getIfPresent(1);
            assertEquals(0, c.stats().requestCount());
        }
    }
}
