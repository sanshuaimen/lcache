package io.lcache;

import java.util.concurrent.TimeUnit;

/**
 * 可注入时钟，供 TTL 过期判断使用。
 * <p>
 * 生产使用单调时钟 {@link System#nanoTime()}（不受系统时钟调整影响）；
 * 测试可注入可拨动的假时钟，从而在不 sleep 的情况下确定性验证过期逻辑
 * （这是"过期行为可测试性"的关键——否则测试只能依赖真实等待）。
 */
@FunctionalInterface
public interface Clock {

    /** 返回当前单调纳秒时间。 */
    long currentTimeNanos();

    /**
     * 返回默认单调时钟（{@code System.nanoTime}）。
     */
    static Clock system() {
        return System::nanoTime;
    }

    /**
     * 校验正时长工具：把给定时间单位换算成纳秒。
     *
     * @param duration 时长数值
     * @param unit     单位
     * @return 换算后的纳秒数
     */
    static long toNanos(long duration, TimeUnit unit) {
        if (duration <= 0) {
            throw new IllegalArgumentException("duration must be positive: " + duration);
        }
        return unit.toNanos(duration);
    }
}
