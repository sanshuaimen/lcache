package io.lcache.benchmark.support;

import java.util.Arrays;
import java.util.random.RandomGenerator;

/**
 * 共享、不可变的 Zipf（幂律）键空间采样器。
 *
 * <p>语义：rank {@code 0} 是最热（权重 1），rank {@code size-1} 最冷（权重 {@code size^-exponent}），
 * 与"业务热度序"一一对应——调用方拿到 rank 后自行映射到真实键（{@code keys[rank]}）。
 *
 * <p>实现：构造期预计算归一化累计概率 {@code prefix[]}（单份、只读），每次采样用
 * {@link Arrays#binarySearch} 逆变换（O(log n) ≈ 18 步，数组常驻 L2/L3）。线程安全：无状态、只读。
 *
 * <p>采样成本约 20–40ns/op。该成本对两个被比较引擎恒定，LOCK_FREE vs SYNCHRONIZED 的
 * 差量结论不受影响；但绝对吞吐会略被压低（相对纯 get 直读），阅读数据时需留意。
 */
public final class ZipfKeySpace {

    private final int size;
    private final double[] prefix;

    public ZipfKeySpace(int size, double exponent) {
        if (size <= 0) {
            throw new IllegalArgumentException("size must be > 0: " + size);
        }
        this.size = size;
        double[] weight = new double[size];
        double sum = 0d;
        for (int r = 0; r < size; r++) {
            weight[r] = 1d / Math.pow(r + 1d, exponent);
            sum += weight[r];
        }
        prefix = new double[size];
        double acc = 0d;
        for (int r = 0; r < size; r++) {
            acc += weight[r] / sum;
            prefix[r] = acc;
        }
        prefix[size - 1] = 1d; // 数值漂移防护
    }

    /** 键空间规模（rank 数）。 */
    public int keySpace() {
        return size;
    }

    /**
     * 采样一个 rank。{@code u} 取 {@code [0,1)}，返回使累计概率恰好盖过 {@code u} 的最小 rank。
     */
    public int sample(RandomGenerator rnd) {
        double u = rnd.nextDouble();
        int hit = Arrays.binarySearch(prefix, u);
        int rank = hit >= 0 ? hit : -hit - 1;
        return rank < size ? rank : size - 1;
    }
}
