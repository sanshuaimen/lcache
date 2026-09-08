package io.lcache;

/**
 * 数据面引擎选择。
 * <p>
 * <ul>
 *   <li>{@link #LOCK_FREE}：默认，包装 SP 无锁哈希表（SizeHashTable）。写路径需在
 *       已注册 ThreadID 的写线程上执行；读路径无锁直读。</li>
 *   <li>{@link #SYNCHRONIZED}：全锁基线，仅用于正确性参照与 JMH 性能对比，
 *       用于量化"无锁 vs 锁"的差距。</li>
 * </ul>
 */
public enum EngineKind {
    /** 无锁哈希表数据面（默认）。 */
    LOCK_FREE,
    /** 全锁哈希表数据面（基线/对比）。 */
    SYNCHRONIZED
}
