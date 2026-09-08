package io.lcache.internal;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * 缓存条目节点：既是引擎存储的值，也承载"值 + TTL + LRU 读计数 + 顺序链表链接"，
 * 由哈希桶（读路径）与顺序链表（维护路径）共同引用——这是本缓存的并发骨架。
 * <p>
 * <b>JMM 说明</b>：
 * <ul>
 *   <li>{@code value / expireAtNanos}：构造后先写入，随后节点通过引擎的无锁 CAS/
 *       volatile 发布到哈希桶——对读者而言，读到节点即看到其构造时的值（happens-before
 *       经发布链传递）；此后值替换通过重新发布新节点完成，读旧节点只读到旧值。</li>
 *   <li>{@code reads}：LRU 读计数，多读者线程经 VarHandle 原子自增，读路径只做这一个
 *       共享写，避免整链重排破坏无锁直读。</li>
 *   <li>{@code prev/next/linked}：仅维护线程在 orderLock 下访问，无需 volatile。</li>
 * </ul>
 */
final class CacheNode<K, V> {

    /** 键（不可变）。 */
    final K key;

    /** 值（volatile，读路径锁无关读取最新值）。 */
    volatile V value;

    /** 过期截止纳秒（0 表示永不过期）。 */
    volatile long expireAtNanos;

    /** LRU 读计数：自节点发布后累计被读的次数，维护时读取并清零。 */
    private volatile int reads;

    // ── 顺序链表链接（仅在 orderLock 下访问） ──
    /** 是否仍在顺序链表中（用于幂等摘除）。 */
    boolean linked;
    CacheNode<K, V> prev;
    CacheNode<K, V> next;

    private static final VarHandle READS;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            READS = l.findVarHandle(CacheNode.class, "reads", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    CacheNode(K key, V value, long expireAtNanos) {
        this.key = key;
        this.value = value;
        this.expireAtNanos = expireAtNanos;
    }

    /** 读路径原子自增读计数（仅 LRU 策略启用时调用）。 */
    void recordRead() {
        READS.getAndAdd(this, 1);
    }

    /** 读取并清零读计数（维护线程在 orderLock 下调用）。 */
    int getAndResetReads() {
        return (int) READS.getAndSet(this, 0);
    }

    boolean isExpired(long nowNanos) {
        long exp = expireAtNanos;
        return exp != 0L && exp <= nowNanos;
    }
}
