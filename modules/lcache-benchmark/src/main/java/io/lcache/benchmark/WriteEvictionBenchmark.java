package io.lcache.benchmark;

import io.lcache.EngineKind;
import io.lcache.LocalCache;
import io.lcache.PolicyKind;
import io.lcache.benchmark.support.ZipfKeySpace;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 维度二：写密集 + 驱逐压力 的工业形态吞吐评测。
 *
 * <p>所有 put/invalidate 收口到线程亲和写池（MutationExecutor），调用方阻塞等待；
 * 两条方法量化写路径的两种成本：
 * <ul>
 *   <li>{@link #putUpdates}：Zipf 流更新"最热 resident 个键"——纯更新（新节点 + put CAS +
 *       orderLock 摘链/入队），隔离写池吞吐上限；</li>
 *   <li>{@link #putCycling}：环形游标穿过远大于 {@code maxSize} 的键空间，缓存满容量起步，
 *       几乎每个 put 都插入新键并逐出队头一个 → 量化 per-op 驱逐成本
 *       （{@code putUpdates − putCycling} ≈ 每次驱逐开销）。</li>
 * </ul>
 *
 * <p>诊断：TearDown 打印 {@code [write-evict] engine/keyModel/max=.../wt=... CacheStats{...}}
 * 到 stderr。put 不计 request，命中率无意义，看 {@code evictionCount}——预期
 * {@code putCycling} 的逐出数 ≈ put 次数（驱逐校验）。统计含 warmup。
 *
 * <p>运行约束：put 阻塞在写池 → in-flight ≤ {@code -t}，保持 {@code -t ≤ 16} 且
 * {@code writerThreads ≥ 4}，避免 MutationExecutor 64/5s 准入拒绝（{@code RejectedExecutionException}）。
 *
 * <p>运行（{@code -p} 收敛矩阵，见 README/run_industrial.sh）：
 * <pre>
 *   java -jar modules/lcache-benchmark/target/lcache-benchmarks.jar \
 *        io.lcache.benchmark.WriteEvictionBenchmark -p engine=LOCK_FREE,SYNCHRONIZED \
 *        -p keyModel=INT -p maxSize=4096,16384 -p writerThreads=1,4,8 \
 *        -t 8 -wi 3 -w 2s -i 5 -r 2s -f 1
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
public class WriteEvictionBenchmark {

    private static final Integer[] VALUES = new Integer[256];

    static {
        for (int i = 0; i < VALUES.length; i++) {
            VALUES[i] = i;
        }
    }

    @Param({"LOCK_FREE", "SYNCHRONIZED"})
    private String engine;
    /**
     * 键空间（环形驱逐穿行的范围），须为 2 的幂。默认 2^18 冒烟档；
     * 旗舰 1GB 驻留 = {@code -p keySpace=67108864 -p maxSize=8388608}。
     */
    @Param({"262144"})
    private int keySpace;
    /** INT：Integer 键；STRING：预构建 {@code "k"+i} 键（Setup 一次建好，op 内零分配）。 */
    @Param({"INT", "STRING"})
    private String keyModel;
    @Param({"4096", "16384"})
    private int maxSize;
    /** 写池线程数（put 在此执行；1..64）。 */
    @Param({"1", "4", "8"})
    private int writerThreads;

    private LocalCache<Object, Object> cache;
    private ZipfKeySpace hotZipf;
    private Object[] keys;
    private AtomicLong ringCursor;
    private int resident;

    /** 桶数：向上取 ≥ max(2^16, 2×驻留) 的 2 的幂（SP 表定容不可扩容；负载 ≈0.5 链短）。 */
    private static int capacityFor(int resident) {
        long want = Math.max(1L << 16, (long) resident << 1);
        long p = Long.highestOneBit(want);
        long c = p < want ? p << 1 : p;
        return c >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) c;
    }

    @Setup(Level.Trial)
    public void setup() {
        resident = Math.min(maxSize, keySpace);
        if (maxSize > keySpace) {
            System.err.println("[warn] write-evict: maxSize(" + maxSize + ") > keySpace(" + keySpace
                    + ")，驻留按 keySpace 截断");
        }
        cache = LocalCache.<Object, Object>builder()
                .capacity(capacityFor(resident))
                .maxSize(maxSize)
                .policy(PolicyKind.LRU)
                .engine(EngineKind.valueOf(engine))
                .writerThreads(writerThreads)
                .recordStats()
                .build();
        keys = new Object[keySpace];
        if (keyModel.equals("INT")) {
            for (int i = 0; i < keySpace; i++) {
                keys[i] = Integer.valueOf(i);
            }
        } else {
            for (int i = 0; i < keySpace; i++) {
                keys[i] = "k" + i;
            }
        }
        hotZipf = new ZipfKeySpace(resident, 1.0d);
        // 预填满 resident 个键，缓存恰好满容量
        for (int i = 0; i < resident; i++) {
            cache.put(keys[i], VALUES[i & 255]);
        }
        // 游标从 resident 起步 → 首个 putCycling 即命中非驻留键，开始逐出
        ringCursor = new AtomicLong(resident);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        try {
            System.err.println("[write-evict] " + engine + "/" + keyModel + "/max=" + maxSize
                    + "/wt=" + writerThreads + " " + cache.stats());
        } finally {
            cache.close();
        }
    }

    /** 每线程采样状态。 */
    @State(Scope.Thread)
    public static class Thd {
        private static final AtomicLong SEEDS = new AtomicLong(0x9E3779B97F4A7C15L);

        final SplittableRandom rnd = new SplittableRandom(SEEDS.getAndAdd(0x9E3779B97F4A7C15L));
    }

    /** 只更新 resident 最热键：隔离纯更新成本。 */
    @Benchmark
    public void putUpdates(Blackhole bh, Thd thd) {
        int rank = hotZipf.sample(thd.rnd);
        bh.consume(cache.put(keys[rank], VALUES[rank & 255]));
    }

    /** 环形新键 churn：每个 put 基本都触发一次驱逐。 */
    @Benchmark
    public void putCycling(Blackhole bh) {
        long i = ringCursor.getAndIncrement();
        int rank = (int) (i & (keySpace - 1));
        bh.consume(cache.put(keys[rank], VALUES[(int) (i & 255)]));
    }
}
