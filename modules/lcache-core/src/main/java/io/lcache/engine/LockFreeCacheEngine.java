package io.lcache.engine;

import algorithms.size.SizeHashTable;

import java.util.Comparator;

/**
 * 无锁数据面：包装 SP 论文级无锁哈希表 {@link SizeHashTable}。
 * <p>
 * SizeHashTable 是"链式无锁哈希表 + per-thread 无锁 size 计数"的一体化实现：
 * <ul>
 *   <li>每个桶是一条按 key 排序的无锁链表，插入/删除用 CAS（{@code NEXT/VAL_OR_REMOVE_INFO}）；</li>
 *   <li>size 计数由 SP 的 per-thread 计数器提供（O(写线程数) 精确、无锁读取）。</li>
 * </ul>
 * <p>
 * <b>使用条件</b>（SP 实现固有）：
 * <ol>
 *   <li>写路径必须在已注册 {@code ThreadID} 的线程上执行——本缓存通过内部写线程池满足；
 *       读路径无需注册；</li>
 *   <li>桶按 key 排序 → key 需实现 {@link Comparable} 或构造时传 {@link Comparator}；</li>
 *   <li>哈希表定容不可扩容 → 构造时确定容量；</li>
 *   <li>null 键/值被底层禁止。</li>
 * </ol>
 *
 * @param <K> 键类型（可排序）
 * @param <E> 条目类型
 */
public final class LockFreeCacheEngine<K, E> implements CacheEngine<K, E> {

    private final SizeHashTable<K, E> table;

    /**
     * @param capacity   哈希表容量（内部向上取 2 的幂，定容不可扩容）
     * @param comparator 键排序比较器；为 null 时要求 K 实现 {@link Comparable}
     */
    public LockFreeCacheEngine(int capacity, Comparator<? super K> comparator) {
        this.table = new SizeHashTable<>(capacity, comparator);
    }

    /** 需在已注册 ThreadID 的写线程上调用。 */
    @Override
    public E put(K key, E entry) {
        return table.put(key, entry);
    }

    /** 需在已注册 ThreadID 的写线程上调用。 */
    @Override
    public E putIfAbsent(K key, E entry) {
        return table.putIfAbsent(key, entry);
    }

    /** 需在已注册 ThreadID 的写线程上调用。 */
    @Override
    public E remove(Object key) {
        return table.remove(key);
    }

    /** 需在已注册 ThreadID 的写线程上调用。 */
    @Override
    public E removeIfValue(Object key, E expected) {
        return table.remove(key, expected) ? expected : null;
    }

    /** 任意线程可调（读路径无锁）。 */
    @Override
    public E get(Object key) {
        return table.get(key);
    }

    @Override
    public boolean containsKey(Object key) {
        return table.containsKey(key);
    }

    @Override
    public int size() {
        return table.size();
    }
}
