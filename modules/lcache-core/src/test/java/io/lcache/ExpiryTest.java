package io.lcache;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TTL 过期测试：注入可拨动时钟做确定性验证（无 sleep）。
 * 语义：读取时惰性判过期（视为 miss），由 cleanUp/维护任务真正移除并回调 EXPIRED。
 */
class ExpiryTest {

    @Test
    void expireAfterWrite_lazyAndSweep() {
        TestClock clock = new TestClock();
        List<RemovalCause> causes = new CopyOnWriteArrayList<>();
        try (LocalCache<String, String> c = LocalCache.<String, String>builder()
                .capacity(1024)
                .expireAfterWrite(1000, TimeUnit.NANOSECONDS)
                .clock(clock)
                .writerThreads(1)
                .removalListener((k, v, cause) -> causes.add(cause))
                .build()) {

            c.put("k", "v");
            assertTrue(c.containsKey("k"));

            clock.advance(500); // 未到期
            assertEquals("v", c.getIfPresent("k"));

            clock.advance(600); // 已超 TTL：惰性判 miss
            assertNull(c.getIfPresent("k"));
            assertFalse(c.containsKey("k"));

            // cleanUp 触发清扫，条目从引擎移除、回调 EXPIRED
            c.cleanUp();
            assertEquals(0, c.size());
            assertTrue(causes.contains(RemovalCause.EXPIRED));
        }
    }

    @Test
    void noExpiry_whenDisabled() {
        try (LocalCache<String, String> c = LocalCache.<String, String>builder()
                .capacity(1024)
                .writerThreads(1)
                .build()) {
            c.put("k", "v");
            assertEquals("v", c.get("k"));
        }
    }

    @Test
    void rePut_afterExpiry_refreshesDeadline() {
        TestClock clock = new TestClock();
        try (LocalCache<String, String> c = LocalCache.<String, String>builder()
                .capacity(1024)
                .expireAfterWrite(1000, TimeUnit.NANOSECONDS)
                .clock(clock)
                .writerThreads(1)
                .build()) {
            c.put("k", "v1");
            clock.advance(2000); // 过期
            assertNull(c.getIfPresent("k"));

            c.put("k", "v2"); // 重新写入：截止时间刷新
            clock.advance(500);
            assertEquals("v2", c.getIfPresent("k"));
        }
    }
}
