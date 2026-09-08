package io.lcache.benchmark;

import io.lcache.EngineKind;
import io.lcache.LocalCache;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

/**
 * 数据面引擎吞吐对比：无锁(SP SizeHashTable) vs 全锁基线，在同一个缓存语义层之上。
 * <p>
 * 读路径在 JMH 线程上无锁直读；写路径收口到缓存的线程亲和写池（4 线程）。
 * <p>
 * 运行：
 *   java -jar modules/lcache-benchmark/target/lcache-benchmarks.jar
 *       io.lcache.benchmark.EngineThroughputBenchmark -wi 2 -w 1s -i 3 -r 1s -f 1
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
public class EngineThroughputBenchmark {

    @Param({"LOCK_FREE", "SYNCHRONIZED"})
    private String engine;

    /** 预装载键空间。 */
    private static final int KEY_SPACE = 1 << 13;

    private LocalCache<Integer, Integer> cache;
    private final AtomicInteger cursor = new AtomicInteger();

    @Setup
    public void setup() {
        cache = LocalCache.<Integer, Integer>builder()
                .capacity(1 << 16)
                .engine(EngineKind.valueOf(engine))
                .writerThreads(4)
                .build();
        // 单线程预填充 KEY_SPACE 个键（写路径经写池）
        for (int i = 0; i < KEY_SPACE; i++) {
            cache.put(i, i);
        }
    }

    @TearDown
    public void tearDown() {
        cache.close();
    }

    private int nextKey() {
        return cursor.getAndIncrement() & (KEY_SPACE - 1);
    }

    /** 读命中：JMH 线程无锁直读。 */
    @Benchmark
    public void getHit(Blackhole bh) {
        bh.consume(cache.getIfPresent(nextKey()));
    }

    /** 读未命中（键空间外的键）：读锁无关 + miss 统计。 */
    @Benchmark
    public void getMiss(Blackhole bh) {
        bh.consume(cache.getIfPresent(KEY_SPACE + nextKey()));
    }

    /** 写已存在键（更新）：经写池，量化写路径开销。 */
    @Benchmark
    public void putUpdate(Blackhole bh) {
        int k = nextKey();
        bh.consume(cache.put(k, k + 1));
    }

    /** 混合 80% 读 / 20% 写，贴近后端请求形态。 */
    @Benchmark
    public void mixedReadWrite(Blackhole bh) {
        int k = nextKey();
        if ((k & 0x7) != 0) {
            bh.consume(cache.getIfPresent(k));
        } else {
            bh.consume(cache.put(k, k + 1));
        }
    }
}
