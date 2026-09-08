package io.lcache.benchmark;

import io.lcache.EngineKind;
import io.lcache.LocalCache;
import io.lcache.PolicyKind;

import java.util.HashMap;
import java.util.Map;

/**
 * 独立（非 JMH）内存占用探测：比较不同引擎容纳同样 {@code n} 个条目时的堆开销。
 *
 * <p>方法：所有 key 用预分配 Integer 池（建表前存活、被所有被测容器共享引用）→ 移出差量；
 * value 在填充时新造且只被被测容器引用 → 计入差量。对每 (payload, engine) 在
 * {@link #SIZES} 的两个驻留规模各填一次，取 GC 沉降后的 used-heap 差：
 * <ul>
 *   <li><b>分摊</b> bytes/entry = 差量 / n（含固定桶阵 / 表）；</li>
 *   <li><b>边际</b> bytes/entry = (大−小)/(n大−n小)（近似单节点成本，剔除固定表）。</li>
 * </ul>
 * 该差量 = 桶/表 + 条目节点 + value payload（不含 key 字节——真实业务 key 需自行加回）。
 *
 * <p>SP 无锁表每桶预分配一个 header Node（固定表），容量按 2 的幂取整、无法缩容；HashMap
 * 自增表。此不对称正是本探测要展示的（"固定桶数摊销"）。堆差量是近似值：
 * 建议 <code>-Xms2g -Xmx2g -XX:+UseSerialGC -XX:+AlwaysPreTouch</code> 运行以获得更稳读数。
 *
 * <p>运行（从 shaded jar 直接以 main 跑，绕过 JMH）：
 * <pre>
 *   java -Xms2g -Xmx2g -XX:+UseSerialGC -XX:+AlwaysPreTouch \
 *        -cp modules/lcache-benchmark/target/lcache-benchmarks.jar \
 *        io.lcache.benchmark.FootprintProbe
 * </pre>
 */
public final class FootprintProbe {

    /** 驻留规模：两个点求边际斜率。 */
    private static final int[] SIZES = {1 << 18, 1 << 19};

    private static final Integer[] KEY_POOL = new Integer[1 << 19];

    static {
        for (int i = 0; i < KEY_POOL.length; i++) {
            KEY_POOL[i] = i;
        }
    }

    /** value payload：新建独立对象（只在容器内被引用）。 */
    private enum Payload {
        INTEGER {
            @Override Object newValue(int seed) {
                return Integer.valueOf(seed + 4096); // 避开 -128..127 常量池，确保每值一个对象
            }
        },
        STRING32 {
            @Override Object newValue(int seed) {
                return new String(SEED_TEXT); // JDK9+ 复制底层 byte[]，每值独立
            }
        },
        BYTE256 {
            @Override Object newValue(int seed) {
                byte[] b = new byte[256];
                b[0] = (byte) seed;
                return b;
            }
        };

        abstract Object newValue(int seed);

        static final String SEED_TEXT = "0123456789abcdef0123456789abcdef"; // 32 chars
    }

    private enum Engine {
        LOCK_FREE("LOCK_FREE"),
        SYNCHRONIZED("SYNCHRONIZED"),
        HASHMAP_BASELINE("HashMap 基线");

        final String name;

        Engine(String name) {
            this.name = name;
        }
    }

    private static final long SETTLE_MS = 60L;

    /** 度量期间把被测容器钉在字段上（写可观测字段防 JIT 逃逸分析/移除局部容器）。 */
    private static Object sink;

    public static void main(String[] args) {
        System.out.println("== lcache FootprintProbe ==");
        System.out.println("读法：差量 = 表/桶 + 条目节点 + value（key 已在建表前预分配、被排除）。");
        System.out.println("建议 -Xms2g -Xmx2g -XX:+UseSerialGC -XX:+AlwaysPreTouch，堆差量为近似值。\n");

        for (Payload p : Payload.values()) {
            // [engine][sizeIdx] 的 used-heap（相对当前 payload 基线的增量）
            long base = measureHeap();
            long[][] used = new long[Engine.values().length][SIZES.length];
            for (int s = 0; s < SIZES.length; s++) {
                int n = SIZES[s];
                for (Engine e : Engine.values()) {
                    used[e.ordinal()][s] = fillAndMeasure(p, e, n) - base;
                    sink = null; // 释放被测容器，让下一轮从干净堆开始
                    System.gc();
                }
            }
            System.out.println("--- payload = " + p.name() + " ---");
            System.out.printf("%-18s %14s %14s%n", "engine", "amortB/entry", "margB/entry");
            for (Engine e : Engine.values()) {
                double large = used[e.ordinal()][1] / (double) SIZES[1];
                double marginal = (used[e.ordinal()][1] - used[e.ordinal()][0])
                        / (double) (SIZES[1] - SIZES[0]);
                // amortB 用大规模值（含固定表的摊销在更大 n 更贴近稳态）
                System.out.printf("%-18s %14.1f %14.1f%n", e.name, large, marginal);
            }
            System.out.println();
        }
    }

    private static long fillAndMeasure(Payload p, Engine e, int n) {
        switch (e) {
            case LOCK_FREE: {
                LocalCache<Integer, Object> c = LocalCache.<Integer, Object>builder()
                        .capacity(n) // SP 表固定桶数 = n（2 的幂）
                        .maxSize(0)
                        .policy(PolicyKind.LRU)
                        .engine(EngineKind.LOCK_FREE)
                        .writerThreads(4)
                        .build();
                try {
                    fillCache(c, p, n);
                    c.cleanUp();
                    sink = c; // 度量期间保持存活
                    return measureHeap();
                } finally {
                    c.close();
                }
            }
            case SYNCHRONIZED: {
                LocalCache<Integer, Object> c = LocalCache.<Integer, Object>builder()
                        .engine(EngineKind.SYNCHRONIZED)
                        .writerThreads(4)
                        .build();
                try {
                    fillCache(c, p, n);
                    c.cleanUp();
                    sink = c;
                    return measureHeap();
                } finally {
                    c.close();
                }
            }
            default: {
                Map<Integer, Object> m = new HashMap<>();
                for (int i = 0; i < n; i++) {
                    m.put(KEY_POOL[i], p.newValue(i));
                }
                sink = m; // 字段写使 m 可观测，防 JIT 把本地 HashMap 整体移除
                return measureHeap();
            }
        }
    }

    private static void fillCache(LocalCache<Integer, Object> c, Payload p, int n) {
        for (int i = 0; i < n; i++) {
            c.put(KEY_POOL[i], p.newValue(i));
        }
    }

    private static long measureHeap() {
        long best = Long.MAX_VALUE;
        for (int trial = 0; trial < 3; trial++) {
            System.gc();
            sleep(SETTLE_MS);
            System.gc();
            sleep(SETTLE_MS);
            Runtime rt = Runtime.getRuntime();
            long used = rt.totalMemory() - rt.freeMemory();
            best = Math.min(best, used);
        }
        return best;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private FootprintProbe() {
    }
}
