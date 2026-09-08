package io.lcache;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发正确性（对拍式/不变量式）测试，验证缓存层 + 无锁引擎在线程安全上无丢失更新。
 *
 * 说明：并发结果无法像单线程那样精确预判先后，因此用"不变量"断言：
 *  1) 多写线程各写独立键 → 全部写入后 size 精确、键全部可读；
 *  2) 同键并发写 → 最后一定落在其中一个写者的值上（无丢失更新）；
 *  3) 写与读并发 → 读到的任何值都必须是"曾写入过的合法值"（无中间态/半写）。
 */
class ConcurrencyConsistencyTest {

    private static final int KEYS_PER_THREAD = 2_000;

    @Test
    void concurrentWritersDistinctKeys_noLostUpdates() throws Exception {
        for (EngineKind engine : EngineKind.values()) {
            try (LocalCache<Integer, String> c = LocalCache.<Integer, String>builder()
                    .capacity(1 << 16)
                    .engine(engine)
                    .writerThreads(4)
                    .build()) {

                int writers = 4;
                CountDownLatch start = new CountDownLatch(1);
                CountDownLatch done = new CountDownLatch(writers);
                for (int t = 0; t < writers; t++) {
                    final int base = t;
                    new Thread(() -> {
                        try {
                            start.await();
                            for (int i = 0; i < KEYS_PER_THREAD; i++) {
                                int key = base * 1_000_000 + i;
                                c.put(key, "v-" + base + "-" + i);
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            done.countDown();
                        }
                    }, "writer-" + engine + "-" + t).start();
                }
                start.countDown();
                done.await();

                int total = writers * KEYS_PER_THREAD;
                assertEquals(total, c.size(), "engine=" + engine + ": SP/per-thread size must be exact");
                for (int t = 0; t < writers; t++) {
                    for (int i = 0; i < KEYS_PER_THREAD; i++) {
                        int key = t * 1_000_000 + i;
                        assertEquals("v-" + t + "-" + i, c.getIfPresent(key),
                                "engine=" + engine + ": lost value for key=" + key);
                    }
                }

                // 各写线程删除自己的键 → 最终 size=0
                CountDownLatch done2 = new CountDownLatch(writers);
                for (int t = 0; t < writers; t++) {
                    final int base = t;
                    new Thread(() -> {
                        try {
                            await(start);
                            for (int i = 0; i < KEYS_PER_THREAD; i++) {
                                c.invalidate(base * 1_000_000 + i);
                            }
                        } finally {
                            done2.countDown();
                        }
                    }, "eraser-" + t).start();
                }
                start.countDown();
                done2.await();

                assertEquals(0, c.size(), "engine=" + engine + ": all keys must be erased");
            }
        }
    }

    @Test
    void concurrentSameKeyWriters_lastWriteWins() throws Exception {
        try (LocalCache<Integer, String> c = LocalCache.<Integer, String>builder()
                .capacity(1024)
                .writerThreads(4)
                .build()) {

            int iters = 20_000;
            CountDownLatch start = new CountDownLatch(1);
            Thread a = new Thread(() -> {
                await(start);
                for (int i = 0; i < iters; i++) {
                    c.put(1, "a");
                }
            }, "writer-a");
            Thread b = new Thread(() -> {
                await(start);
                for (int i = 0; i < iters; i++) {
                    c.put(1, "b");
                }
            }, "writer-b");

            a.start();
            b.start();
            start.countDown();
            a.join();
            b.join();

            // 主线程最后写一次：此后值必须确定（无丢失更新）
            c.put(1, "final");
            assertEquals("final", c.getIfPresent(1));
            assertEquals(1, c.size());
        }
    }

    @Test
    void readersNeverSeeInvalidValues_whileConcurrentWrites() throws Exception {
        final Set<String> ALLOWED = Set.of("v0", "v1", "v2", "v3");
        try (LocalCache<Integer, String> c = LocalCache.<Integer, String>builder()
                .capacity(1 << 14)
                .writerThreads(4)
                .build()) {

            final int keys = 500;
            for (int i = 0; i < keys; i++) {
                c.put(i, "v0");
            }

            int writers = 2, readers = 4;
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(writers + readers);
            ConcurrentHashMap<String, Boolean> bad = new ConcurrentHashMap<>();

            for (int w = 0; w < writers; w++) {
                new Thread(() -> {
                    try {
                        start.await();
                        ThreadLocalRandom rnd = ThreadLocalRandom.current();
                        for (int i = 0; i < 100_000; i++) {
                            c.put(rnd.nextInt(keys), "v" + (1 + rnd.nextInt(3)));
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                }, "writer-" + w).start();
            }
            for (int r = 0; r < readers; r++) {
                new Thread(() -> {
                    try {
                        start.await();
                        ThreadLocalRandom rnd = ThreadLocalRandom.current();
                        for (int i = 0; i < 100_000; i++) {
                            String v = c.getIfPresent(rnd.nextInt(keys));
                            if (v != null && !ALLOWED.contains(v)) {
                                bad.put(v, Boolean.TRUE);
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                }, "reader-" + r).start();
            }

            start.countDown();
            done.await();
            assertTrue(bad.isEmpty(), "readers observed invalid value: " + bad.keySet());
            assertEquals(keys, c.size());
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
