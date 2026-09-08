package io.lcache;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 测试用可拨动时钟：用原子递增模拟单调纳秒时间，避免测试 sleep。
 * 配合 {@link CacheBuilder#clock(Clock)} 注入到缓存中做确定性过期验证。
 */
public final class TestClock implements Clock {

    private final AtomicLong now = new AtomicLong(1_000_000L);

    @Override
    public long currentTimeNanos() {
        return now.get();
    }

    /** 时间前进 deltaNanos。 */
    public void advance(long deltaNanos) {
        now.addAndGet(deltaNanos);
    }
}
