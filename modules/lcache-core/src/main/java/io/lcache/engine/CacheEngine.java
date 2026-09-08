package io.lcache.engine;

/**
 * 数据面引擎抽象：缓存语义层不感知具体并发实现，只通过该 SPI 做最基本的 K→条目
 * 存取。条目类型 E 由实现层决定（本缓存用 CacheNode 承载值 + 元数据）。
 * <p>
 * 无锁引擎要求：{@link #put}/{@link #remove} 系列只能在已注册 ThreadID 的写线程
 * （线程亲和写池）上执行；{@link #get}/{@link #size} 任意线程可调、无需注册。
 */
public interface CacheEngine<K, E> {

    /**
     * 写入映射（返回被替换的旧条目，未命中返回 null）。
     * 线性化点：底层无锁表对 value 的 CAS。
     */
    E put(K key, E entry);

    /** 仅当键不存在时写入（返回已存在条目则未写入）。 */
    E putIfAbsent(K key, E entry);

    /** 删除键（返回被删条目或 null）。 */
    E remove(Object key);

    /**
     * 仅当键当前映射的条目 {@code == expected} 时才删除（返回 expected 或 null）。
     * 用于"安全驱逐/过期"：防止删掉被并发替换出的新条目。
     */
    E removeIfValue(Object key, E expected);

    /** 取当前条目或 null。 */
    E get(Object key);

    /** 键是否存在。 */
    boolean containsKey(Object key);

    /** 当前条目数。 */
    int size();
}
