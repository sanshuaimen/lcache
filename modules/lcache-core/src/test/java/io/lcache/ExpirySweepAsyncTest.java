package io.lcache;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TTL <b>异步过期清扫</b>路径测试（真实维护任务，而非显式 {@code cleanUp()}）。
 *
 * <p>现有 {@link ExpiryTest} 全部依赖手动 {@code cleanUp()} 触发清扫，走的是同步强制全维护
 * （{@code runMaintenance()}）；但生产后端只有后台时间闸清扫在跑——{@code put}/{@code get}
 * 命中过期条目时经合并闸 + 时间闸把一次 {@code expireSweep()} 投递到写线程异步执行。
 * 本测试用可拨动时钟 + 真实线程调度覆盖这条<b>无人显式调用</b>的收敛路径。
 *
 * <p>可拨动时钟只推进"逻辑时间"（TTL 判定依据）；清扫任务仍由真实写线程异步执行，
 * 用 {@link CountDownLatch} 等待确定性收敛，无 sleep。
 */
class ExpirySweepAsyncTest {

    /** TTL：足够大让过期判断与清扫间隔都清晰可控（清扫间隔 = ttl/8，夹在 [1ms,1s]）。 */
    private static final long TTL_NANOS = TimeUnit.MILLISECONDS.toNanos(500);
    private static final int N = 5;

    private LocalCache<Integer, String> build(TestClock clock, AtomicInteger expires,
                                              CountDownLatch expiredLatch) {
        return LocalCache.<Integer, String>builder()
                .capacity(1024)
                .expireAfterWrite(TTL_NANOS, TimeUnit.NANOSECONDS)
                .clock(clock)
                .recordStats()
                .writerThreads(1)
                .removalListener((k, v, cause) -> {
                    if (cause == RemovalCause.EXPIRED) {
                        expires.incrementAndGet();
                        expiredLatch.countDown();
                    }
                })
                .build();
    }

    private void fill(LocalCache<Integer, String> c) {
        for (int i = 0; i < N; i++) {
            c.put(i, "v-" + i);
        }
    }

    @Test
    void expiredEntries_areSweptAsynchronously_withoutCleanUp() throws Exception {
        TestClock clock = new TestClock();
        AtomicInteger expires = new AtomicInteger();
        CountDownLatch expiredAll = new CountDownLatch(N);
        try (LocalCache<Integer, String> c = build(clock, expires, expiredAll)) {
            fill(c);
            assertEquals(N, c.size());

            // 推过全部截止时间；不做任何 cleanUp，仅靠一次惰性读触发异步清扫
            clock.advance(TTL_NANOS + 1_000_000);
            assertNull(c.getIfPresent(0));       // 惰性 miss → 投递一次 expireSweep 到写线程

            // 清扫任务在写线程异步执行：等待 EXPIRED 回调全部到达
            assertTrue(expiredAll.await(5, TimeUnit.SECONDS),
                    "async sweeper must remove all expired entries and fire EXPIRED callbacks");
            assertEquals(N, expires.get());
            assertEquals(0, c.size(), "sweeper must converge engine size to exact after removal");
            assertFalse(c.containsKey(0));
            assertEquals(N, c.stats().expireCount());
        }
    }

    @Test
    void sizeCountsUnsweptExpiredEntries_untilSweepConverges() throws Exception {
        TestClock clock = new TestClock();
        AtomicInteger expires = new AtomicInteger();
        CountDownLatch expiredAll = new CountDownLatch(N);
        try (LocalCache<Integer, String> c = build(clock, expires, expiredAll)) {
            fill(c);

            // 推过截止时间但期间无任何读/写触发清扫：过期条目在"判定 miss 与清扫移除"之间仍被计数
            // （README §8 边界 #4 的确定性复现）
            clock.advance(TTL_NANOS + 1_000_000);
            assertEquals(N, c.size(), "expired-but-unswept entries must still be counted by size()");

            // 第一次过期读是惰性 miss，并触发异步清扫；收敛后 size 精确
            assertNull(c.getIfPresent(0));
            assertTrue(expiredAll.await(5, TimeUnit.SECONDS));
            assertEquals(N, expires.get());
            assertEquals(0, c.size(), "size() converges to exact only after the sweep runs");
        }
    }
}
