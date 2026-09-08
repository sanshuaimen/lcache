package io.lcache;

/**
 * 条目被移除的原因，供 {@link RemovalListener} 感知"为什么没缓存了"。
 * <p>
 * 该枚举服务于后端场景语义：命中率下降排查 / 失效一致性通知 / 容量规划。
 */
public enum RemovalCause {
    /** 显式失效（{@code invalidate/remove}），例如"写库后失效缓存"。 */
    EXPLICIT,
    /** TTL 到期，被惰性判定为 miss 或由维护任务清扫。 */
    EXPIRED,
    /** 达到容量上限 {@code maxSize} 后被驱逐。 */
    EVICTED
}
