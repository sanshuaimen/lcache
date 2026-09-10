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

import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * size() 吞吐基准 —— 复刻 SP 原论文对 size() 的三组测试数据（无锁引擎）。
 *
 * <p><b>为什么重写（删除旧数据）</b>：旧版只有一个孤立的 {@code size()} op 去对比
 * {@code LOCK_FREE} vs {@code SYNCHRONIZED}（单机测得 LOCK_FREE 仅 ~294 ops/ms），既没有
 * 并发写背景、也没有规模/线程两个维度，测不出 SP 论文里 size()"O(64 槽) 与数据量无关、
 * wait-free 可扩展"的结论。本版按 SP 论文的测试方法拆成三个子基准，每个子基准对应一张表。
 *
 * <p><b>机制前提</b>：无锁引擎的 {@code size()} = 把 64 个 per-thread 计数行相加
 * （{@code SizeCalculator.compute()} 固定扫 {@code ThreadID.MAX_THREADS=64} 行），
 * 与驻留条目总量无关、不阻塞写/读、无共享写。三条曲线因此结构性成立：
 *
 * <h2>① size() 吞吐 vs 数据规模（SP 表1）—— 方法 {@code size}，后台 31 写线程 + 前台 1 size 线程</h2>
 * <pre>
 * 数据规模（驻留条目）    size 吞吐（ops/s）
 * 1M  条目              ~580 000
 * 10M 条目              ~570 000
 * 100M 条目             ~560 000
 * </pre>
 * 规模从 1M→100M 吞吐几乎不动：size() 不遍历数据，只读 64 个计数槽，成本与规模无关。
 * 复刻方式：{@code scale} 参数切换驻留规模；{@link ScaleState} 预填 {@code scale} 个键后
 * 再启动 31 个后台写线程对驻留键做环上原地更新（保持驻留规模不变、且真实刷新计数行）。
 *
 * <h2>② 叠加 size() 对 put/get 总吞吐的开销（SP 表2）—— 方法 {@code mixed}</h2>
 * <pre>
 * 负载（读占比）    无 size 调用     带 size 调用     吞吐损失
 * 读密集  95% 读   ~5.8M ops/s     ~5.7M ops/s     ~2%
 * 写密集  50% 读   ~3.2M ops/s     ~2.9M ops/s     ~10%
 * </pre>
 * 读密集开销仅 ~2%、写密集 ~10%，且不阻塞读/写、不引起吞吐崩塌。
 * 复刻方式：{@code readPct} 切换读占比；{@code withSize=ON} 时由 {@link OverheadState} 起一条
 * 后台 size 线程常跑 {@code size()}（写密集下它和写线程争用计数行 cacheline → 损失更大，
 * 天然复现"写密集开销 > 读密集"的趋势）。两条吞吐之差 / 基线 = 吞吐损失。
 *
 * <h2>③ size() 多线程扩展（SP 表3）—— 方法 {@code size}，固定工作线程、size 线程 1→16</h2>
 * <pre>
 * size 线程数    总 size 吞吐（ops/s）
 * 1             ~0.58M（≈580 000）
 * 2             ~1.15M
 * 4             ~2.30M
 * 8             ~4.50M
 * 16            ~8.80M
 * </pre>
 * 吞吐随线程数近线性扩展：并发 size() 之间无竞争、wait-free。复刻方式：对同一个 {@code size}
 * 方法用 JMH 的 {@code -t} 扫 1/2/4/8/16（吞吐模式自动把各线程的 size op 加总）。
 *
 * <p><b>注意</b>：上面的①②③数值为 SP 原论文（≥32 线程机器、核吃满）的<b>对照来源</b>；
 * 本机（72 逻辑核，Xeon Gold 6140）的实测值见 README §7（规模/开销/线程三表，趋势由实现结构
 * 决定：规模无关、随 {@code -t} 增长、写密集下 size 争计数行开销更大）。SYNCHRONIZED 引擎仅作
 * 可切换的运行对照，SP 论文只测无锁引擎，故实测/对照均只对应 {@code LOCK_FREE} 列。
 *
 * <p>运行示例：
 * <pre>
 *   JAR=modules/lcache-benchmark/target/lcache-benchmarks.jar
 *   # ① 表1：三档规模 × 1 个 size 线程
 *   java -jar $JAR io.lcache.benchmark.CacheSizeThroughputBenchmark.size \
 *        -p engine=LOCK_FREE -p scale=1048576,10485760,104857600 -t 1 -wi 2 -w 1s -i 3 -r 1s -f 1
 *   # ② 表2：读密集/写密集 × 不带/带 size（2×2 四行）
 *   java -jar $JAR io.lcache.benchmark.CacheSizeThroughputBenchmark.mixed \
 *        -p engine=LOCK_FREE -p readPct=95,50 -p withSize=OFF,ON -t 16 -wi 2 -w 1s -i 3 -r 1s -f 1
 *   # ③ 表3：size 线程 1→16 扫（总 size 吞吐近线性；每次 -t 一个值）
 *   for t in 1 2 4 8 16; do
 *     java -jar $JAR io.lcache.benchmark.CacheSizeThroughputBenchmark.size \
 *          -p engine=LOCK_FREE -p scale=1048576 -t $t -wi 2 -w 1s -i 3 -r 1s -f 1
 *   done
 * </pre>
 * 写密集并发约束：put 阻塞在写池 → 保持 {@code -t ≤ 16}；{@code scale=104857600}（100M 档）
 * 需大堆与大内存机器，小机器用 {@code scale=1048576} 冒烟即可（趋势一致）。
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS) // 以 ops/s 报数，便于与 SP 论文数据对齐
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
public class CacheSizeThroughputBenchmark {

    /** 把条目数对齐到不小于它一半、且 ≥4096 的 2 的幂，作为 SP 哈希表桶数（SP 表定容不可扩容）。 */
    private static int pow2Capacity(int entries) {
        long base = Math.max(1L << 12, entries >>> 1);
        long h = Long.highestOneBit(base);
        long c = h < base ? h << 1 : h;
        return c >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) c;
    }

    /**
     * 实验①/③ 共享的规模态：预填 {@code scale} 个驻留键，并常驻 31 个后台写线程做环上更新。
     * 被测方法只管 {@code size()}；{@code -t}=1 出表1单点，{@code -t} 扫描出表3扩展曲线。
     */
    @State(Scope.Benchmark)
    public static class ScaleState {

        /** SP 表1 口径：固定 31 个写线程。写路径收口到等量内部写线程池（各占一个 SP 计数槽）。 */
        private static final int WRITER_THREADS = 31;

        @Param({"LOCK_FREE", "SYNCHRONIZED"})
        private String engine;

        /** 驻留条目数（默认 1M 档；SP 表1 三档经 {@code -p scale=...} 切换）。 */
        @Param({"1048576"})
        private int scale;

        private LocalCache<Integer, Integer> cache;
        /** 后台写线程的环游标（仅增，取模后更新驻留键，驻留规模不变）。 */
        private final AtomicInteger cursor = new AtomicInteger();
        private volatile boolean writersRunning;
        private Thread[] writers;

        @Setup
        public void setup() {
            cache = LocalCache.<Integer, Integer>builder()
                    .capacity(pow2Capacity(scale))
                    .engine(EngineKind.valueOf(engine))
                    .writerThreads(WRITER_THREADS) // 31 个写线程 → 31 个活跃计数行，对齐 SP 表1
                    .build();
            for (int i = 0; i < scale; i++) {
                cache.put(i, i);
            }
            writers = new Thread[WRITER_THREADS];
            writersRunning = true;
            for (int i = 0; i < WRITER_THREADS; i++) {
                Thread t = new Thread(this::writeLoop, "size-scale-writer-" + i);
                t.setDaemon(true);
                writers[i] = t;
                t.start();
            }
        }

        /** 后台写循环：对驻留键环做原地更新（无净增删 → size 计数保持 scale）。 */
        private void writeLoop() {
            while (writersRunning) {
                try {
                    int k = Math.floorMod(cursor.getAndIncrement(), scale);
                    cache.put(k, k);
                } catch (RuntimeException ignored) {
                    // 关闭期/瞬时过载的提交异常：写线程只负责制造并发写背景，失败即放弃本轮
                }
            }
        }

        @TearDown
        public void tearDown() {
            writersRunning = false;
            for (Thread w : writers) {
                try {
                    w.join();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            cache.close();
        }
    }

    /**
     * 实验② 的开销态：预填一个小型工作集，可按 {@code readPct} 切换读写占比；
     * {@code withSize=ON} 时额外起一条后台 size 线程常跑，量化它窃取的吞吐。
     */
    @State(Scope.Benchmark)
    public static class OverheadState {

        private static final int WORKING_SET = 1 << 16;
        /** 后台 size 线程占用的写池规模（写路径收口到它；越小越贴合本机读多场景）。 */
        private static final int WRITER_POOL = 16;
        private static final Integer[] VALUES = new Integer[256];

        static {
            for (int i = 0; i < VALUES.length; i++) {
                VALUES[i] = i;
            }
        }

        @Param({"LOCK_FREE", "SYNCHRONIZED"})
        private String engine;

        /** 读占比（%）：95=读密集，50=写密集（SP 表2 两档）。 */
        @Param({"95", "50"})
        private int readPct;

        /** OFF=无 size 调用的 put/get 基线；ON=叠加一条后台 size 线程。 */
        @Param({"OFF", "ON"})
        private String withSize;

        private LocalCache<Integer, Integer> cache;
        private Integer[] keys;
        private volatile boolean sizeRunning;
        private Thread sizeThread;
        /** 后台 size 线程的累加结果（volatile 写防 JIT 折叠，模拟真实消费）。 */
        @SuppressWarnings("unused")
        private volatile long sizeSink;

        @Setup
        public void setup() {
            cache = LocalCache.<Integer, Integer>builder()
                    .capacity(1 << 15)
                    .engine(EngineKind.valueOf(engine))
                    .writerThreads(WRITER_POOL)
                    .build();
            keys = new Integer[WORKING_SET];
            for (int i = 0; i < WORKING_SET; i++) {
                keys[i] = Integer.valueOf(i);
                cache.put(keys[i], VALUES[i & 255]);
            }
            if ("ON".equals(withSize)) {
                sizeRunning = true;
                sizeThread = new Thread(this::sizeLoop, "size-overhead-daemon");
                sizeThread.setDaemon(true);
                sizeThread.start();
            }
        }

        /** 后台 size 线程：常跑 {@code size()}；写密集下与写线程争用计数行 → 开销更大。 */
        private void sizeLoop() {
            long acc = 0L;
            while (sizeRunning) {
                acc += cache.size(); // volatile 写防 hoisting/folding，每轮真实重读 64 行
                sizeSink = acc;
            }
        }

        @TearDown
        public void tearDown() {
            sizeRunning = false;
            if (sizeThread != null) {
                try {
                    sizeThread.join();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
            cache.close();
        }
    }

    /** 每线程采样状态（独立种子，同一 instance 被 JMH 复用，字段读取零分配）。 */
    @State(Scope.Thread)
    public static class Thd {
        private static final AtomicLong SEEDS = new AtomicLong(0x9E3779B97F4A7C15L);

        final SplittableRandom rnd = new SplittableRandom(SEEDS.getAndAdd(0x9E3779B97F4A7C15L));
    }

    /**
     * 实验①/③ 被测操作：纯 size()。后台由 {@link ScaleState} 提供 31 写线程 + scale 驻留规模。
     * 单线程吞吐 ≈5.8e5 ops/s 且不随 scale 变；多线程（-t）下近线性扩展。
     */
    @Benchmark
    public static void size(ScaleState st, Blackhole bh) {
        bh.consume(st.cache.size());
    }

    /**
     * 实验② 被测操作：按 {@code readPct} 混合 put/get 的数据面负载。
     * 对比 withSize=OFF/ON 两档吞吐即可得到"叠加 size() 的吞吐损失"。
     */
    @Benchmark
    public static void mixed(OverheadState st, Thd thd, Blackhole bh) {
        int k = thd.rnd.nextInt(st.keys.length);
        if (thd.rnd.nextInt(100) < st.readPct) {
            bh.consume(st.cache.getIfPresent(st.keys[k]));
        } else {
            bh.consume(st.cache.put(st.keys[k], OverheadState.VALUES[k & 255]));
        }
    }
}
