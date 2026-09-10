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
 * 维度三：TTL 过期 + <b>后台异步清扫</b>对吞吐影响的工业形态评测。
 *
 * <p>业务背景：TTL 开启后，过期条目由维护任务按<b>时间闸摊销</b>做全链清扫（间隔 ≈ ttl/8，
 * 夹在 [1ms, 1s]，见 README §2）。清扫以 O(驻留集) 成本周期性运行并占用写线程池；本基准用
 * {@code ttlUs} 参数把 <b>有 TTL（含清扫）</b>与 <b>无 TTL（零清扫，对照）</b>隔离出来：
 * <ul>
 *   <li>{@code ttlUs=0}：不过期 → 写路径零调度零清扫，是纯净更新基线；</li>
 *   <li>{@code ttlUs>0}：Zipf 刷新流只更新 resident 热键 → 低频键自然老化，被时间闸驱动的
 *       周期清扫回收；TTL 越小，回收越频繁，清扫占用的写吞吐越多。</li>
 * </ul>
 *
 * <p>诊断：TearDown 打印 {@code [ttl-sweep] engine/max/ttlUs=... CacheStats{...}}（含 expires 计数）。
 * 观察点：同配置下 {@code ttlUs>0} 相对 {@code ttlUs=0} 的吞吐差 ≈ 过期清扫 + 回收写的总代价；
 * {@code expires>0} 确认清扫确实发生。
 *
 * <p>运行约束：put 阻塞在写池 → 保持 {@code -t ≤ 16}。
 *
 * <pre>
 *   java -jar modules/lcache-benchmark/target/lcache-benchmarks.jar \
 *        io.lcache.benchmark.TtlExpirySweepBenchmark -p engine=LOCK_FREE,SYNCHRONIZED \
 *        -p maxSize=4096,16384 -p ttlUs=0,1000 -t 8 -wi 3 -w 2s -i 5 -r 2s -f 1
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
public class TtlExpirySweepBenchmark {

    private static final int KEY_SPACE = 1 << 18;
    private static final int CAPACITY = 1 << 16;

    private static final Integer[] VALUES = new Integer[256];

    static {
        for (int i = 0; i < VALUES.length; i++) {
            VALUES[i] = i;
        }
    }

    @Param({"LOCK_FREE", "SYNCHRONIZED"})
    private String engine;
    /** 驻留集上限（驱逐 + TTL 清扫的共同回收目标）。 */
    @Param({"4096", "16384"})
    private int maxSize;
    /** per-entry TTL（微秒）；0 = 关闭 TTL（零清扫对照）。 */
    @Param({"0", "1000"})
    private long ttlUs;

    private LocalCache<Object, Object> cache;
    private ZipfKeySpace zipf;
    private Object[] keys;

    @Setup(Level.Trial)
    public void setup() {
        io.lcache.CacheBuilder<Object, Object> b = LocalCache.<Object, Object>builder()
                .capacity(CAPACITY)
                .maxSize(maxSize)
                .policy(PolicyKind.LRU)
                .engine(EngineKind.valueOf(engine))
                .writerThreads(4)
                .recordStats();
        if (ttlUs > 0) {
            b.expireAfterWrite(ttlUs, TimeUnit.MICROSECONDS);
        }
        cache = b.build();

        int resident = Math.min(maxSize, KEY_SPACE);
        keys = new Object[KEY_SPACE];
        for (int i = 0; i < KEY_SPACE; i++) {
            keys[i] = Integer.valueOf(i);
        }
        zipf = new ZipfKeySpace(resident, 1.0d);
        // 预填满 resident 个热键：ttl>0 时 warmup 内即收敛到"热键常驻 + 低频键老化"稳态
        for (int i = 0; i < resident; i++) {
            cache.put(keys[i], VALUES[i & 255]);
        }
    }

    /** Zipf 刷新流：只更新最热 resident 键；低频键在 {@code ttlUs>0} 时自然老化并被清扫回收。 */
    @Benchmark
    public void putUpdates(Blackhole bh, Thd thd) {
        int rank = zipf.sample(thd.rnd);
        bh.consume(cache.put(keys[rank], VALUES[rank & 255]));
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        try {
            System.err.println("[ttl-sweep] " + engine + "/max=" + maxSize
                    + "/ttlUs=" + ttlUs + " " + cache.stats());
        } finally {
            cache.close();
        }
    }

    /** 每线程采样状态：独立种子。 */
    @State(Scope.Thread)
    public static class Thd {
        private static final AtomicLong SEEDS = new AtomicLong(0x9E3779B97F4A7C15L);

        final SplittableRandom rnd = new SplittableRandom(SEEDS.getAndAdd(0x9E3779B97F4A7C15L));
    }
}
