package io.lcache;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 容量驱逐测试（确定性：单调用线程顺序操作，写池=1，行为等价于串行语义）。
 *
 * 设计说明：FIFO/LRU 的区别点在于"读是否影响淘汰顺序"。
 * 由于读路径不回写 LRU 链表（保无锁直读），LRU 通过"读计数 + cleanUp/维护提升"近似；
 * 因此 LRU 测试需要先读热点键、再 cleanUp() 触发提升，然后超容量插入验证淘汰对象。
 */
class EvictionPolicyTest {

    @Test
    void fifo_evictsOldestRegardlessOfReads() {
        List<RemovalCause> causes = new CopyOnWriteArrayList<>();
        try (LocalCache<String, String> c = LocalCache.<String, String>builder()
                .capacity(1024)
                .maxSize(3)
                .policy(PolicyKind.FIFO)
                .writerThreads(1)
                .removalListener((k, v, cause) -> causes.add(cause))
                .build()) {
            c.put("a", "1");
            c.put("b", "2");
            c.put("c", "3");

            // 即便频繁读 a，FIFO 也不改变淘汰顺序
            for (int i = 0; i < 100; i++) {
                c.get("a");
            }
            c.cleanUp();

            c.put("d", "4"); // 触发驱逐，应淘汰最老的 a
            assertNull(c.getIfPresent("a"));
            assertEquals("2", c.getIfPresent("b"));
            assertEquals("3", c.getIfPresent("c"));
            assertEquals("4", c.getIfPresent("d"));
            assertEquals(3, c.size());
            assertTrue(causes.contains(RemovalCause.EVICTED));
        }
    }

    @Test
    void lru_keepsRecentlyReadHotKey() {
        try (LocalCache<String, String> c = LocalCache.<String, String>builder()
                .capacity(1024)
                .maxSize(3)
                .policy(PolicyKind.LRU)
                .writerThreads(1)
                .build()) {
            c.put("a", "1");
            c.put("b", "2");
            c.put("c", "3");

            // 频繁读 a（提升为最近使用）
            for (int i = 0; i < 100; i++) {
                c.getIfPresent("a");
            }
            c.cleanUp(); // 维护：把读计数高的 a 移到队尾

            c.put("d", "4"); // 触发驱逐，应淘汰 b（a 已被提升）
            assertNotNull(c.getIfPresent("a"));
            assertNull(c.getIfPresent("b"));
            assertEquals("3", c.getIfPresent("c"));
            assertEquals("4", c.getIfPresent("d"));
            assertEquals(3, c.size());
        }
    }

    @Test
    void capacityNeverExceedsMaxSize_afterWrites() {
        try (LocalCache<Integer, String> c = LocalCache.<Integer, String>builder()
                .capacity(4096)
                .maxSize(64)
                .policy(PolicyKind.LRU)
                .writerThreads(2)
                .build()) {
            for (int i = 0; i < 10_000; i++) {
                c.put(i, "v-" + i);
            }
            c.cleanUp();
            assertEquals(64, c.size()); // cleanUp 后容量严格收口到 maxSize
        }
    }

    @Test
    void updateExistingKey_doesNotGrowSize() {
        try (LocalCache<String, String> c = LocalCache.<String, String>builder()
                .capacity(1024)
                .maxSize(2)
                .policy(PolicyKind.FIFO)
                .writerThreads(1)
                .build()) {
            c.put("k", "1");
            c.put("k", "2"); // 更新，不新增条目
            c.put("k", "3");
            assertEquals(1, c.size());
            assertEquals("3", c.getIfPresent("k"));
        }
    }
}
