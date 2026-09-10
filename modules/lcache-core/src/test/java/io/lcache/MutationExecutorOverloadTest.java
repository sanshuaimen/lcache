package io.lcache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 写路径<b>过载故障注入</b>测试（对应 README §2"写池过载保护"的承诺）：
 *
 * <p>后端最怕的回归不是"慢"，而是"慢到看不到、直到某天 OOM"——本测试把写线程<b>真正卡死</b>
 * （用驱逐回调在写线程上阻塞），制造一个有界准入闸（Semaphore=64）被填满、写任务队列饱和的
 * 过载现场，验证：
 * <ol>
 *   <li>非写池调用方<b>不会无限阻塞</b>：闸位拿不到时按 2×5s 提交超时 + 200ms 重试等待后
 *       快速失败抛 {@link RejectedExecutionException}（≈10s，非无穷等待、非静默积压）；</li>
 *   <li>被拒绝的那次写<b>不生效</b>（抛错发生在入队之前、未执行，调用方重试安全）；</li>
 *   <li>写线程解除卡死后缓存<b>自动恢复</b>：排队中的写被逐批执行、后续写正常，无数据损坏。</li>
 * </ol>
 *
 * <p>故障注入方式：`maxSize=1 + writerThreads=1`，第二个 put 触发驱逐，驱逐回调（在写线程上
 * 内联执行）阻塞 → 写线程卡死。随后 80 个调用方并发各写一个唯一键：64 个获得闸位并在队列等待，
 * 其余 ~16 个经历两轮 5s 提交超时后快速失败——因此本测试耗时约 10–12s，属预期。</p>
 */
class MutationExecutorOverloadTest {

    /** 有界准入闸默认上限（见 MutationExecutor#DEFAULT_MAX_PENDING_WRITES），测试须超过它。 */
    private static final int ADMIT_LIMIT = 64;
    /** 并发调用方数：大于闸上限，保证有调用方拿不到闸位而走快速失败路径。 */
    private static final int CALLERS = 80;

    private static final int SEED_KEY = 1;
    private static final int STALL_KEY = 2;
    private static final int FLOOD_KEY_BASE = 1_000;

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void writerStall_saturatesAdmissionGate_thenFastFailsAndRecovers() throws Exception {
        AtomicBoolean firstEviction = new AtomicBoolean();
        CountDownLatch writerEntered = new CountDownLatch(1);   // 确认写线程已卡在回调内
        CountDownLatch releaseWriter = new CountDownLatch(1);   // 主线程解除卡死

        try (LocalCache<Integer, String> c = LocalCache.<Integer, String>builder()
                .capacity(4096)
                .maxSize(1)               // 每次 put 新键都驱逐队头 → 驱逐回调可作为写线程卡点
                .policy(PolicyKind.FIFO)
                .writerThreads(1)
                .removalListener((k, v, cause) -> {
                    // 只卡住第一次驱逐（即卡死写线程的那一次）；之后的驱逐不再阻塞
                    if (firstEviction.compareAndSet(false, true)) {
                        writerEntered.countDown();
                        try {
                            releaseWriter.await(60, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                })
                .build()) {

            // ── 阶段 1：让写线程卡死在驱逐回调内 ──
            c.put(SEED_KEY, "seed");                  // size==1==maxSize，暂无驱逐
            Thread staller = new Thread(() -> c.put(STALL_KEY, "stall"), "staller");
            staller.setDaemon(true);
            staller.start();                          // 驱逐 SEED_KEY → 回调阻塞写线程
            assertTrue(writerEntered.await(10, TimeUnit.SECONDS),
                    "writer should be blocked inside removal callback (eviction of seed)");

            // ── 阶段 2：并发洪峰，填满闸位并触发快速失败 ──
            CountDownLatch go = new CountDownLatch(1);
            AtomicInteger rejected = new AtomicInteger();
            AtomicLong firstRejectNanos = new AtomicLong();  // 第一个失败的耗时（应≈2×5s 提交超时）
            ConcurrentLinkedQueue<Integer> rejectedKeys = new ConcurrentLinkedQueue<>();
            List<Thread> callers = new java.util.ArrayList<>();
            for (int i = 0; i < CALLERS; i++) {
                final int key = FLOOD_KEY_BASE + i;
                Thread t = new Thread(() -> {
                    try {
                        go.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    long t0 = System.nanoTime();
                    try {
                        c.put(key, "x-" + key);       // 阻塞等写池：拿到闸位的会一直等
                    } catch (RejectedExecutionException re) {
                        rejected.incrementAndGet();
                        firstRejectNanos.compareAndSet(0, System.nanoTime() - t0);
                        rejectedKeys.add(key);
                    }
                }, "flood-" + i);
                t.setDaemon(true);
                callers.add(t);
                t.start();
            }

            // 同时发令；闸位 64 < 调用方 80 → 必有 ~16 个调用方拿不到闸位
            go.countDown();

            // 等待快速失败真正出现（两轮 5s 提交超时 ≈ 10.2s）；这是"有界、非无限阻塞"的证据
            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
            while (rejected.get() == 0 && System.nanoTime() < deadlineNanos) {
                Thread.sleep(20);
            }

            // 无论断言成败都要释放写线程，避免测试挂死
            releaseWriter.countDown();
            staller.join(TimeUnit.SECONDS.toMillis(10));
            for (Thread t : callers) {
                t.join(TimeUnit.SECONDS.toMillis(20));
            }

            // ── 断言 1：过载必须快速失败（而非无限阻塞/静默积压） ──
            assertTrue(rejected.get() > 0,
                    "admission gate must saturate and fail fast with RejectedExecutionException "
                            + "when writer is stalled; observed 0 rejections");

            // ── 断言 2：失败耗时符合"两轮 5s 提交超时"设计（≈10s，而非立即/超长） ──
            long firstNs = firstRejectNanos.get();
            assertTrue(firstNs >= TimeUnit.SECONDS.toNanos(8)
                            && firstNs < TimeUnit.SECONDS.toNanos(30),
                    "first rejected submit should wait ~2 submit timeouts (~10s) before failing, "
                            + "was " + TimeUnit.NANOSECONDS.toMillis(firstNs) + "ms");

            // ── 断言 3：被拒绝的那次写不生效（抛错发生在入队前、未执行） ──
            for (Integer k : rejectedKeys) {
                assertNull(c.getIfPresent(k),
                        "rejected put for key=" + k + " must not take effect (never enqueued)");
            }

            // ── 断言 4：写线程解除卡死后自动恢复，无数据损坏 ──
            c.put(SEED_KEY, "recovered");
            assertEquals("recovered", c.getIfPresent(SEED_KEY));
            assertEquals(1, c.size());
        }
    }
}
