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
 * 维度一：热点倾斜 · 读多写少 的工业形态吞吐评测。
 *
 * <p>键分布为 Zipf（s=1，rank 0 最热），键空间 2^18 ≫ {@code maxSize}，因此：
 * <ul>
 *   <li>驻留集 = 最热的 maxSize 个键（Setup 预填），队尾键读请求构成真实的稳定 miss 底；</li>
 *   <li>{@code maxSize=0} 时全量驻留 → 无驱逐上限参照（≈100% 命中）；</li>
 *   <li>方法内的少量 put 充当"被逐出热键的治愈写"，测量窗口已到稳态。</li>
 * </ul>
 *
 * <p>命中率诊断：TearDown 打印一行 {@code [hot-read] engine/keyModel/max=... CacheStats{...}}
 * 到 stderr（含 hitRate 与 evictionCount）。统计跨整个 fork（含 warmup），warmup 瞬态相对
 * 百万级读可忽略，误差约 1–2%；{@code recordStats()} 会给每次读加两次 LongAdder 自增，
 * 报出的 ops/s 含少量"统计税"——要纯引擎吞吐，用同一配置把 {@code .recordStats()} 关掉再跑一次。
 *
 * <p>读扩展曲线用 {@link #read}（纯读、不吃写池，读吞吐随 {@code -t} 近线性）；
 * 真实读写混合用 {@link #mixed95_5}/{@link #mixed80_20}（写路径收口写池，保持 {@code -t ≤ 16}）。
 * 旗舰 1GB 驻留档：{@code -p keySpace=67108864 -p maxSize=8388608}（INT 键）。
 *
 * <p>运行（矩阵较大，建议用 {@code -p} 收敛；见 README/run_industrial.sh）：
 * <pre>
 *   java -jar modules/lcache-benchmark/target/lcache-benchmarks.jar \
 *        io.lcache.benchmark.HotReadZipfBenchmark.read -p engine=LOCK_FREE -p keyModel=INT \
 *        -p keySpace=67108864 -p maxSize=8388608 -t 1,8,16,32,64 -wi 3 -w 1s -i 5 -r 1s -f 1
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
public class HotReadZipfBenchmark {

    private static final Integer[] VALUES = new Integer[256];

    static {
        for (int i = 0; i < VALUES.length; i++) {
            VALUES[i] = i;
        }
    }

    @Param({"LOCK_FREE", "SYNCHRONIZED"})
    private String engine;
    /**
     * 键空间（含大量"永远 miss"的队尾键），须为 2 的幂。默认 2^18 冒烟档；
     * 旗舰 1GB 驻留 = {@code -p keySpace=67108864 -p maxSize=8388608}（JMH {@code -p} 可给 @Param 之外的值）。
     */
    @Param({"262144"})
    private int keySpace;
    /** INT：Integer 键；STRING：预构建 {@code "k"+i} 键（Setup 一次建好，op 内零分配）。 */
    @Param({"INT", "STRING"})
    private String keyModel;
    /** 0 = 不驱逐（全量驻留上限参照）；>0 制造淘汰压力。 */
    @Param({"0", "4096", "16384"})
    private int maxSize;

    private LocalCache<Object, Object> cache;
    private ZipfKeySpace zipf;
    private Object[] keys;

    /** 桶数：向上取 ≥ max(2^16, 2×驻留) 的 2 的幂（SP 表定容不可扩容；负载 ≈0.5 链短）。 */
    private static int capacityFor(int resident) {
        long want = Math.max(1L << 16, (long) resident << 1);
        long p = Long.highestOneBit(want);
        long c = p < want ? p << 1 : p;
        return c >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) c;
    }

    @Setup(Level.Trial)
    public void setup() {
        // 驻留 = maxSize（热集）；maxSize=0 → 全量 keySpace 驻留；maxSize>keySpace → 截断防越界
        int prefill = maxSize > 0 ? Math.min(maxSize, keySpace) : keySpace;
        if (maxSize > keySpace) {
            System.err.println("[warn] hot-read: maxSize(" + maxSize + ") > keySpace(" + keySpace
                    + ")，驻留按 keySpace 截断");
        }
        cache = LocalCache.<Object, Object>builder()
                .capacity(capacityFor(prefill))
                .maxSize(maxSize)
                .policy(PolicyKind.LRU)
                .engine(EngineKind.valueOf(engine))
                .writerThreads(4)
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
        zipf = new ZipfKeySpace(keySpace, 1.0d);
        // 预填最热的 prefill 个键；maxSize=0 时全量驻留
        for (int i = 0; i < prefill; i++) {
            cache.put(keys[i], VALUES[i & 255]);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        try {
            System.err.println("[hot-read] " + engine + "/" + keyModel + "/max=" + maxSize
                    + " " + cache.stats());
        } finally {
            cache.close();
        }
    }

    /** 每线程采样状态：独立种子（同一 instance 被 JMH 复用，字段读取零分配）。 */
    @State(Scope.Thread)
    public static class Thd {
        private static final AtomicLong SEEDS = new AtomicLong(0x9E3779B97F4A7C15L);

        final SplittableRandom rnd = new SplittableRandom(SEEDS.getAndAdd(0x9E3779B97F4A7C15L));
    }

    /** 95% 读 / 5% 写。 */
    @Benchmark
    public void mixed95_5(Blackhole bh, Thd thd) {
        int rank = zipf.sample(thd.rnd);
        Object k = keys[rank];
        if (thd.rnd.nextInt(100) < 5) {
            bh.consume(cache.put(k, VALUES[rank & 255]));
        } else {
            bh.consume(cache.getIfPresent(k));
        }
    }

    /** 80% 读 / 20% 写。 */
    @Benchmark
    public void mixed80_20(Blackhole bh, Thd thd) {
        int rank = zipf.sample(thd.rnd);
        Object k = keys[rank];
        if (thd.rnd.nextInt(100) < 20) {
            bh.consume(cache.put(k, VALUES[rank & 255]));
        } else {
            bh.consume(cache.getIfPresent(k));
        }
    }

    /**
     * 纯读：100% {@code getIfPresent}，不触写池/驱逐 → 读吞吐随 {@code -t} 近线性扩展，
     * 不被写池限速。仍保留 Zipf 冷尾 miss → hitRate ≈ 理论 ln(maxSize)/ln(keySpace)
     * （旗舰 2^23/2^26 ≈ 0.885；maxSize=0 时全命中）。
     */
    @Benchmark
    public void read(Blackhole bh, Thd thd) {
        int rank = zipf.sample(thd.rnd);
        bh.consume(cache.getIfPresent(keys[rank]));
    }
}
