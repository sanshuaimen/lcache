package io.lcache.demo;

import io.lcache.CacheStats;
import io.lcache.EngineKind;
import io.lcache.LocalCache;
import io.lcache.PolicyKind;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 后端请求场景演示（进程内，无网络层）。
 *
 * 演示四件事，全部围绕"进程内 L1 缓存被后端业务线程并发调用"的真实形态：
 *   Phase 1  基础语义 + 写后失效（后端缓存一致性用法）；
 *   Phase 2  读多写少的"请求线程池"负载（read-through + 写后失效），给出吞吐与命中率；
 *   Phase 3  无锁引擎 vs 全锁基线在相同负载下的吞吐对比（量化无锁收益）；
 *   Phase 4  Zipf 访问下 FIFO vs 近似 LRU 的命中率对比（驱逐策略质量）。
 *
 * 运行：mvn -pl modules/lcache-demo -am exec:java
 */
public final class DemoRunner {

    // 模拟的"数据源/数据库"：真实后端里缓存前面就是这个。
    private static final ConcurrentHashMap<Integer, String> DB = new ConcurrentHashMap<>();

    /** 假装的数据库读取（微延迟，模拟磁盘/远程源）。 */
    private static String readThroughSource(Integer key) {
        String v = DB.get(key);
        if (v == null) {
            // 未命中"库"则写一条（模拟初次计算/落库）
            v = "data-" + key + "-" + System.nanoTime();
            String raced = DB.putIfAbsent(key, v);
            if (raced != null) {
                v = raced;
            }
        }
        // 模拟几十微秒的源端延迟
        try {
            Thread.sleep(0, 20_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return v;
    }

    /** Zipf 风格热点选择器：少量键高频访问，模拟真实后端热点。 */
    private static final class HotKeyGen {
        private final Random rnd;
        private final int hotSize;
        private final int coldBound;

        HotKeyGen(int hotSize, int coldBound, long seed) {
            this.hotSize = hotSize;
            this.coldBound = coldBound;
            this.rnd = new Random(seed);
        }

        int next() {
            // ~90% 命中热点区，~10% 冷键
            if (rnd.nextDouble() < 0.9d) {
                return rnd.nextInt(hotSize);
            }
            return hotSize + rnd.nextInt(coldBound - hotSize);
        }
    }

    public static void main(String[] args) throws Exception {
        demoQuickSemantics();
        runBackendLoadScenario(EngineKind.LOCK_FREE, "无锁引擎 LOCK_FREE", 8, 15_000);
        runBackendLoadScenario(EngineKind.SYNCHRONIZED, "全锁基线 SYNCHRONIZED", 8, 15_000);
        runZipfHitRateProbe();
        System.out.println("\n[lcache-demo] 结束。");
    }

    // ── Phase 1：语义演示 ─────────────────────────────────────────
    private static void demoQuickSemantics() {
        System.out.println("\n================ Phase 1  基础语义 + 写后失效 ================");
        try (LocalCache<Integer, String> cache = LocalCache.<Integer, String>builder()
                .capacity(1 << 12)
                .maxSize(3)
                .policy(PolicyKind.LRU)
                .writerThreads(2)
                .removalListener((k, v, cause) ->
                        System.out.println("  [removal] key=" + k + " value=" + v + " cause=" + cause))
                .recordStats()
                .build()) {
            cache.put(1, "one");
            cache.put(2, "two");
            cache.put(3, "three");
            System.out.println("size = " + cache.size() + "  (精确 size：SP per-thread 计数)");

            // 读热点 1 号键（LRU 提升）
            for (int i = 0; i < 50; i++) {
                cache.getIfPresent(1);
            }
            cache.cleanUp();
            cache.put(4, "four"); // 触发驱逐
            System.out.println("put(4) 后 size = " + cache.size() + "，get(1)=" + cache.get(1)
                    + "，get(2)=" + cache.getIfPresent(2) + "  (2 应被 LRU 淘汰)");

            // 写库后失效：模拟后端更新 DB → 失效缓存 → 下次读穿重新加载
            String old = cache.get(5, DemoRunner::readThroughSource);
            System.out.println("读穿首次 get(5) -> " + old);
            DB.put(5, "fresh-from-db");
            cache.invalidate(5);
            String fresh = cache.get(5, DemoRunner::readThroughSource);
            System.out.println("失效后重新读穿 get(5) -> " + fresh);
            System.out.println("stats: " + cache.stats());
        }
    }

    // ── Phase 2/3：请求线程池负载 + 引擎对比 ───────────────────────
    private static void runBackendLoadScenario(EngineKind engine, String label,
                                               int clients, int itersPerClient) throws InterruptedException {
        System.out.println("\n================ " + label + " ================");
        // 每次场景独立 DB，保证可复现
        DB.clear();
        final int hotSize = 2000;
        final int coldBound = 200_000;

        try (LocalCache<Integer, String> cache = LocalCache.<Integer, String>builder()
                .capacity(1 << 18)
                .maxSize(4096)
                .policy(PolicyKind.LRU)
                .engine(engine)
                .writerThreads(4)
                .recordStats()
                .build()) {

            AtomicLong totalOps = new AtomicLong();
            ExecutorService pool = Executors.newFixedThreadPool(clients);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(clients);
            for (int c = 0; c < clients; c++) {
                final int clientId = c;
                pool.submit(() -> {
                    try {
                        HotKeyGen gen = new HotKeyGen(hotSize, coldBound, 42L + clientId);
                        Random rnd = new Random();
                        start.await();
                        for (int i = 0; i < itersPerClient; i++) {
                            int key = gen.next();
                            double r = rnd.nextDouble();
                            if (r < 0.85d) {
                                // 读穿：命中直接返回；未命中单飞加载（防击穿）
                                cache.get(key, DemoRunner::readThroughSource);
                            } else if (r < 0.95d) {
                                // 写后失效：更新"库"再失效缓存（缓存一致性）
                                DB.put(key, "updated-" + key);
                                cache.invalidate(key);
                            } else {
                                // 偶发缓存击穿/加载失败路径的热身
                                cache.getIfPresent(key);
                            }
                            totalOps.incrementAndGet();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        done.countDown();
                    }
                });
            }

            long t0 = System.nanoTime();
            start.countDown();
            done.await();
            pool.shutdown();
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000;

            CacheStats s = cache.stats();
            double opsPerSec = totalOps.get() * 1000.0d / Math.max(1, elapsedMs);
            System.out.printf("  ops=%d  耗时=%.2fs  吞吐=%,.0f ops/s%n",
                    totalOps.get(), elapsedMs / 1000.0d, opsPerSec);
            System.out.printf("  命中率=%.4f  驱逐=%d  过期=%d  加载=%d(失败 %d)  平均加载=%.1fus  cache.size=%d%n",
                    s.hitRate(), s.evictionCount(), s.expireCount(),
                    s.loadCount(), s.loadFailureCount(), s.averageLoadPenaltyNanos() / 1000.0d, cache.size());
        }
    }

    // ── Phase 4：Zipf 命中率对比 ──────────────────────────────────
    private static void runZipfHitRateProbe() {
        System.out.println("\n================ Phase 4  Zipf 访问：FIFO vs 近似 LRU 命中率 ================");
        final int keySpace = 2000;
        final int maxSize = 256;
        final int reads = 300_000;

        List<double[]> rows = new ArrayList<>();
        rows.add(probe(PolicyKind.FIFO, keySpace, maxSize, reads));
        rows.add(probe(PolicyKind.LRU, keySpace, maxSize, reads));

        System.out.printf("  %-6s %12s %12s%n", "策略", "命中", "命中率");
        for (double[] row : rows) {
            System.out.printf("  %-6s %12.0f %11.4f%%%n", (row[2] == 1 ? "LRU" : "FIFO"),
                    row[0], row[0] / reads * 100.0d);
        }
    }

    /** 跑一遍 Zipf 读，返回 [命中数, 未命中数, 0=FIFO,1=LRU]；周期性 cleanUp 让 LRU 生效。 */
    private static double[] probe(PolicyKind policy, int keySpace, int maxSize, int reads) {
        try (LocalCache<Integer, String> cache = LocalCache.<Integer, String>builder()
                .capacity(1 << 13)
                .maxSize(maxSize)
                .policy(policy)
                .writerThreads(2)
                .build()) {
            Random rnd = new Random(7L);
            long hit = 0;
            for (int i = 0; i < reads; i++) {
                // Zipf 近似：指数衰减选取
                int key = zipfKey(rnd, keySpace);
                if (cache.getIfPresent(key) != null) {
                    hit++;
                } else {
                    cache.put(key, "v"); // 未命中则写回（read-through 语义）
                }
                if ((i & 0xFF) == 0) {
                    cache.cleanUp(); // 让 LRU 读计数提升落地
                }
            }
            return new double[]{hit, reads - hit, policy == PolicyKind.LRU ? 1 : 0};
        }
    }

    /** 指数型 Zipf 近似：P(k) ∝ e^{-λk}，λ=0.02，重热低键、长尾高键。 */
    private static int zipfKey(Random rnd, int n) {
        double u = rnd.nextDouble();
        double x = -Math.log(Math.max(1e-12, 1.0 - u)) / 0.02;
        return Math.min(n - 1, (int) x);
    }
}
