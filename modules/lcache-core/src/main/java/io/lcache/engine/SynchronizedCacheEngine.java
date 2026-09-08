package io.lcache.engine;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 全锁数据面（基线）：单把全局 {@link ReentrantLock} + {@link HashMap}。
 * <p>
 * 用途：
 * <ul>
 *   <li>正确性参照——最朴素、最不易错的实现，作为并发测试的 oracle 基线；</li>
 *   <li>JMH 对比——量化"无锁哈希表 vs 全局锁"在同语义缓存层下的差距，直接呼应
 *       本项目的无锁/JMM 学习主线。</li>
 * </ul>
 * 该引擎不依赖 SP，也不要求注册 ThreadID；但在缓存层中它同样只在写线程池上被调用。
 */
public final class SynchronizedCacheEngine<K, E> implements CacheEngine<K, E> {

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<K, E> map = new HashMap<>();

    @Override
    public E put(K key, E entry) {
        lock.lock();
        try {
            return map.put(key, entry);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E putIfAbsent(K key, E entry) {
        lock.lock();
        try {
            return map.putIfAbsent(key, entry);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E remove(Object key) {
        lock.lock();
        try {
            return map.remove(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E removeIfValue(Object key, E expected) {
        lock.lock();
        try {
            E cur = map.get(key);
            if (cur == expected) {
                map.remove(key);
                return expected;
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E get(Object key) {
        lock.lock();
        try {
            return map.get(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean containsKey(Object key) {
        lock.lock();
        try {
            return map.containsKey(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int size() {
        lock.lock();
        try {
            return map.size();
        } finally {
            lock.unlock();
        }
    }
}
