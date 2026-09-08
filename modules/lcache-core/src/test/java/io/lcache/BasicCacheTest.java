package io.lcache;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 基础语义测试：put/get/remove/invalidate/putIfAbsent/size/containsKey。
 * 分别在无锁引擎与全锁基线上验证一致行为（数据面引擎对缓存语义透明）。
 */
class BasicCacheTest {

    private LocalCache<Integer, String> build(EngineKind engine) {
        return LocalCache.<Integer, String>builder()
                .capacity(1024)
                .engine(engine)
                .writerThreads(1)
                .build();
    }

    @Test
    void basicPutGetRemove_onBothEngines() {
        for (EngineKind engine : EngineKind.values()) {
            try (LocalCache<Integer, String> c = build(engine)) {
                assertNull(c.getIfPresent(1));
                assertNull(c.put(1, "one"));
                assertEquals("one", c.getIfPresent(1));
                assertEquals("one", c.get(1));
                assertTrue(c.containsKey(1));

                // 覆盖写返回旧值
                assertEquals("one", c.put(1, "ONE"));
                assertEquals("ONE", c.get(1));
                assertEquals(1, c.size());

                // remove / invalidate
                assertEquals("ONE", c.invalidate(1));
                assertFalse(c.containsKey(1));
                assertNull(c.get(1));
                assertEquals(0, c.size());

                // 空删除返回 null
                assertNull(c.invalidate(999));
            }
        }
    }

    @Test
    void putIfAbsent_semantics() {
        try (LocalCache<Integer, String> c = build(EngineKind.LOCK_FREE)) {
            assertNull(c.putIfAbsent(1, "a"));
            assertEquals("a", c.putIfAbsent(1, "b")); // 已存在，不覆盖，返回旧值
            assertEquals("a", c.get(1));
            assertEquals(1, c.size());
        }
    }

    @Test
    void nullGuards_areRejected() {
        try (LocalCache<Integer, String> c = build(EngineKind.LOCK_FREE)) {
            assertThrows(NullPointerException.class, () -> c.put(null, "x"));
            assertThrows(NullPointerException.class, () -> c.put(1, null));
            assertThrows(NullPointerException.class, () -> c.get((Integer) null));
            assertThrows(NullPointerException.class, () -> c.invalidate(null));
        }
    }

    @Test
    void builderValidation() {
        assertThrows(IllegalArgumentException.class,
                () -> LocalCache.builder().expireAfterWrite(-1, TimeUnit.SECONDS));
        assertThrows(IllegalArgumentException.class,
                () -> LocalCache.builder().capacity(0));
        assertThrows(IllegalArgumentException.class,
                () -> LocalCache.<Integer, String>builder().writerThreads(0));
        assertThrows(IllegalArgumentException.class,
                () -> LocalCache.<Integer, String>builder().writerThreads(65));
    }
}
