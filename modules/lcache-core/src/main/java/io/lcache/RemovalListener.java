package io.lcache;

/**
 * 条目移除监听器（后端场景：写库后失效、驱逐/过期通知、监控计数）。
 * <p>
 * 回调发生在执行移除的写线程（维护线程）上，不在用户读线程上同步执行；
 * 若回调内部异常，将被捕获记录，不影响缓存主流程。
 */
@FunctionalInterface
public interface RemovalListener<K, V> {

    /**
     * 当某条目因 {@link RemovalCause} 被移除时回调。
     *
     * @param key   被移除的键（非空）
     * @param value 被移除时的值（可能为 null，若该值已被替换/清空）
     * @param cause 移除原因
     */
    void onRemoval(K key, V value, RemovalCause cause);
}
