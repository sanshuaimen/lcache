package io.lcache;

import java.util.function.Function;

/**
 * 本地缓存门面接口（纯内存，无网络层）。
 * <p>
 * 线程模型约定（详见实现类注释）：
 * <ul>
 *   <li><b>读路径</b>：{@link #getIfPresent(Object)} 等无锁直读，任何线程可直接调用；</li>
 *   <li><b>写路径</b>：{@link #put}/{@link #invalidate} 收口到内部线程亲和写线程池执行
 *       （该池线程已注册 SP 的 per-thread 计数槽），调用方阻塞等待结果。</li>
 * </ul>
 * 语义口径：读与写并发时允许"宽松一致性"——读到刚被移除的值或已过期值视为 miss 属正常
 * 后端缓存行为；写操作在真正落库那刻线性化。
 */
public interface LocalCache<K, V> extends AutoCloseable {

    /** 返回键到值的映射；不存在或已过期返回 {@code null}。 */
    V get(K key);

    /**
     * 读穿：命中返回缓存值；未命中则单飞加载（同一 key 只允许一个线程执行 loader，
     * 其余线程等待结果），加载成功后写入缓存。防止缓存击穿/惊群。
     *
     * @throws NullPointerException 若 loader 返回 null
     */
    V get(K key, Function<? super K, ? extends V> loader);

    /** 同 {@link #get(Object)}，但不会触发加载。 */
    V getIfPresent(K key);

    /** 键当前是否可命中（含过期检查）。 */
    boolean containsKey(K key);

    /** 写入缓存，返回旧值（若不存在返回 {@code null}）。 */
    V put(K key, V value);

    /** 仅当键不存在时写入，返回旧值；若已存在则不覆盖。 */
    V putIfAbsent(K key, V value);

    /**
     * 显式失效单个键（后端"写库后失效缓存"语义），返回被移除的值。
     * 触发 {@link RemovalListener}，原因 {@link RemovalCause#EXPLICIT}。
     */
    V invalidate(K key);

    /**
     * 当前条数。基于数据面引擎的计数器：
     * 无锁引擎为 O(写线程数) 精确计数；含未清扫的过期条目（最终一致，{@link #cleanUp()} 后精确）。
     */
    int size();

    /** 统计快照；未开启统计时返回近似 0 的空统计。 */
    CacheStats stats();

    /**
     * 触发一次同步维护：过期清扫 + LRU 读计数提升 + 容量驱逐收口。
     * 返回时维护已完成。用于测试确定性、以及后台低频清扫的收口入口。
     */
    void cleanUp();

    /** 关闭写线程池（优雅停止）。之后写操作将拒绝。 */
    @Override
    void close();

    /** 构造一个配置器。 */
    static <K, V> CacheBuilder<K, V> builder() {
        return new CacheBuilder<>();
    }
}
