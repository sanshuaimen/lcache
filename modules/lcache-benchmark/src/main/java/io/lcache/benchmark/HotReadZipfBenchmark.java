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
 * <p>运行（矩阵较大，建议用 {@code -p} 收敛；见 README/run_industrial.sh）：
 * <pre>
 *   java -jar modules/lcache-benchmark/target/lcache-benchmarks.jar \
 *        io.lcache.benchmark.HotReadZipfBenchmark -p engine=LOCK_FREE -p keyModel=INT \
 *        -p maxSize=4096,16384 -t 8 -wi 3 -w 2s -i 5 -r 2s -f 1
 * </pre>
 * 写密集并发约束：put 阻塞在写池 → 保持 {@code -t ≤ 16}。
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
public class HotReadZipfBenchmark {

    /** 键空间（含大量"永远 miss"的队尾键）。 */
    private static final int KEY_SPACE = 1 << 18;
    /** 显式桶数（SP 表固定、按 2 的幂取整）；≥ 最大驻留集即可。 */
    private static final int CAPACITY = 1 << 16;

    private static final Integer[] VALUES = new Integer[256];

    static {
        for (int i = 0; i < VALUES.length; i++) {
            VALUES[i] = i;
        }
    }

    @Param({"LOCK_FREE", "SYNCHRONIZED"})
    private String engine;
    /** INT：Integer 键；STRING：预构建 {@code "k"+i} 键（Setup 一次建好，op 内零分配）。 */
    @Param({"INT", "STRING"})
    private String keyModel;
    /** 0 = 不驱逐（全量驻留上限参照）；>0 制造淘汰压力。 */
    @Param({"0", "4096", "16384"})
    private int maxSize;

    private LocalCache<Object, Object> cache;
    private ZipfKeySpace zipf;
    private Object[] keys;

    @Setup(Level.Trial)
    public void setup() {
        int prefill = maxSize > 0 ? maxSize : KEY_SPACE;
        cache = LocalCache.<Object, Object>builder()
                .capacity(CAPACITY)
                .maxSize(maxSize)
                .policy(PolicyKind.LRU)
                .engine(EngineKind.valueOf(engine))
                .writerThreads(4)
                .recordStats()
                .build();
        keys = new Object[KEY_SPACE];
        if (keyModel.equals("INT")) {
            for (int i = 0; i < KEY_SPACE; i++) {
                keys[i] = Integer.valueOf(i);
            }
        } else {
            for (int i = 0; i < KEY_SPACE; i++) {
                keys[i] = "k" + i;
            }
        }
        zipf = new ZipfKeySpace(KEY_SPACE, 1.0d);
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
}
