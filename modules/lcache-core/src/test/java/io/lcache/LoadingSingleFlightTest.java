package io.lcache;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 读穿单飞加载测试：同一 key 并发 get(key, loader) 时 loader 只执行一次（防击穿）。
 */
class LoadingSingleFlightTest {

    @Test
    void loaderExecutedOnlyOnce_underConcurrentGets() throws Exception {
        try (LocalCache<Integer, String> c = LocalCache.<Integer, String>builder()
                .capacity(1024)
                .recordStats()
                .writerThreads(2)
                .build()) {

            int threads = 8;
            AtomicInteger loads = new AtomicInteger();
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            List<String> results = new ArrayList<>();

            for (int i = 0; i < threads; i++) {
                new Thread(() -> {
                    try {
                        start.await();
                        String r = c.get(42, k -> {
                            loads.incrementAndGet();
                            try {
                                Thread.sleep(50); // 放大 leader 执行窗口
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            return "loaded-" + k;
                        });
                        synchronized (results) {
                            results.add(r);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        done.countDown();
                    }
                }, "loader-client-" + i).start();
            }

            start.countDown();
            done.await();

            assertEquals(1, loads.get(), "loader must run exactly once");
            assertEquals(threads, results.size());
            assertTrue(results.stream().allMatch("loaded-42"::equals));
            assertEquals("loaded-42", c.getIfPresent(42));
            assertEquals(1, c.size());
            assertTrue(c.stats().loadCount() == 1);
        }
    }

    @Test
    void loaderFailure_propagatesAndDoesNotCache() {
        try (LocalCache<Integer, String> c = LocalCache.<Integer, String>builder()
                .capacity(1024)
                .writerThreads(2)
                .build()) {
            assertThrows(IllegalStateException.class, () -> c.get(7, k -> {
                throw new IllegalStateException("boom-" + k);
            }));
            assertTrue(c.getIfPresent(7) == null);
        }
    }

    @Test
    void loaderReturningNull_isRejected() {
        try (LocalCache<Integer, String> c = LocalCache.<Integer, String>builder()
                .capacity(1024)
                .writerThreads(2)
                .build()) {
            assertThrows(NullPointerException.class, () -> c.get(8, k -> null));
        }
    }
}
