package io.lcache.benchmark;

import io.lcache.LocalCache;
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
 * 维度四：读穿单飞 {@code get(key, loader)} 的<b>并发去重压测</b>。
 *
 * <p>业务背景：缓存 miss 时同一 key 的并发请求应合并为<b>一次</b> loader 执行（leader 加载、
 * follower 等结果），否则后端 DB/上游会被同 key 的惊群打垮（缓存击穿）。{@code lcache-core}
 * 的 {@code LoadingSingleFlightTest} 已证单 key 正确性；本基准在 JMH 多线程并发下量化
 * <b>去重率</b>与吞吐：每次 op 先 {@code invalidate} 再 {@code get(key, loader)}，让缓存保持
 * 冷 miss，制造大量并发同 key 竞争。
 *
 * <p>参数作用：{@code keySpace} 越小、{@code -t} 越大、{@code delayUs} 越大（leader 执行窗口
 * 越宽），同 key 请求越容易重叠 → 去重率应越高。
 *
 * <p>诊断：TearDown 打印一行
 * {@code [single-flight] keySpace=../delayUs=../ loads=.. misses=.. dedup=XX.X% CacheStats{...}}。
 * 看 {@code dedup = 1 - loadCount/missCount}：loads 是真正执行了 loader 的次数（leader），
 * misses 是走到加载路径的请求总数（含 follower）——去重率越高说明 leader/follower 合并越有效。
 * 若该值异常趋近 0%（每次 miss 都各自 load），即单飞去重失效。
 *
 * <pre>
 *   java -jar modules/lcache-benchmark/target/lcache-benchmarks.jar \
 *        io.lcache.benchmark.SingleFlightLoadBenchmark -p keySpace=16,256 -p delayUs=0,300 \
 *        -t 16 -wi 3 -w 2s -i 5 -r 2s -f 1
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
public class SingleFlightLoadBenchmark {

    /** 同 key 竞争基数：越小并发碰撞越密、去重率越高。 */
    @Param({"16", "256"})
    private int keySpace;
    /** loader 人工执行耗时（微秒）：拉宽 leader 执行窗口，放大 follower 重叠。 */
    @Param({"0", "300"})
    private long delayUs;

    private LocalCache<Integer, Integer> cache;
    private Integer[] keys;
    private final AtomicLong loaderCalls = new AtomicLong();

    @Setup(Level.Trial)
    public void setup() {
        cache = LocalCache.<Integer, Integer>builder()
                .capacity(1 << 14)
                .writerThreads(4)
                .recordStats()
                .build();
        keys = new Integer[keySpace];
        for (int i = 0; i < keySpace; i++) {
            keys[i] = i;
        }
        loaderCalls.set(0);
    }

    /** 每次 get 前先 invalidate：让缓存保持冷 miss，制造并发加载竞争。 */
    @Benchmark
    public void invalidateThenLoad(Blackhole bh, Thd thd) {
        Integer k = keys[thd.rnd.nextInt(keySpace)];
        bh.consume(cache.invalidate(k));   // 写库后失效：制造下一次 miss
        bh.consume(cache.get(k, this::load)); // 并发同 key 只应有一次走 load
    }

    /** leader 实际执行的加载：计数 + 人工延迟以拉宽竞争窗口。 */
    private Integer load(Integer key) {
        loaderCalls.incrementAndGet();
        if (delayUs > 0) {
            long deadline = System.nanoTime() + delayUs * 1_000L;
            while (System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
        }
        return key;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        try {
            io.lcache.CacheStats s = cache.stats();
            long loads = loaderCalls.get();
            long misses = s.missCount();
            double dedup = misses == 0 ? 0d : 100d * (1d - (double) loads / misses);
            System.err.println("[single-flight] keySpace=" + keySpace + "/delayUs=" + delayUs
                    + " loads=" + loads + " misses=" + misses
                    + String.format(" dedup=%.1f%%", dedup)
                    + " " + s);
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
