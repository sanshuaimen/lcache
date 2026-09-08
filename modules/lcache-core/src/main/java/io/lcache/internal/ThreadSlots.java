package io.lcache.internal;

/**
 * 线程槽常量：SP per-thread 计数器的最大槽数。
 * <p>
 * 对应 SP 源 {@code ThreadID.MAX_THREADS = 64}。写线程池大小不得超过此值，
 * 因为每个写线程须占用一个独立的计数器行（见 CacheBuilder#writerThreads 约束）。
 */
public final class ThreadSlots {

    /** SP per-thread 计数器的最大槽数。 */
    public static final int MAX_SLOTS = 64;

    private ThreadSlots() { }
}
