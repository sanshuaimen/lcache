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

import java.util.concurrent.TimeUnit;

/**
 * size() 成本对比。
 * <ul>
 *   <li>无锁引擎：SP per-thread 计数器求和，O(写线程数)、无锁、读线程直接算；</li>
 *   <li>全锁基线：取 HashMap.size()，需抢全局锁（与其它读/写串行）。</li>
 * </ul>
 * 运行：
 *   java -jar modules/lcache-benchmark/target/lcache-benchmarks.jar
 *       io.lcache.benchmark.CacheSizeThroughputBenchmark -t 8 -wi 2 -w 1s -i 3 -r 1s -f 1
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
public class CacheSizeThroughputBenchmark {

    @Param({"LOCK_FREE", "SYNCHRONIZED"})
    private String engine;

    private LocalCache<Integer, Integer> cache;

    @Setup
    public void setup() {
        cache = LocalCache.<Integer, Integer>builder()
                .capacity(1 << 16)
                .engine(EngineKind.valueOf(engine))
                .writerThreads(4)
                .build();
        for (int i = 0; i < (1 << 12); i++) {
            cache.put(i, i);
        }
    }

    @TearDown
    public void tearDown() {
        cache.close();
    }

    @Benchmark
    public void size(Blackhole bh) {
        bh.consume(cache.size());
    }
}
