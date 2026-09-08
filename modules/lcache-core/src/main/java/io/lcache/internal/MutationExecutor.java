package io.lcache.internal;

import measurements.support.ThreadID;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * 线程亲和写执行器：收口所有写/维护路径的线程池。
 * <p>
 * 为什么必须存在（对应 SP 数据面的使用条件）：无锁引擎 {@code SizeHashTable} 的写路径
 * 需要在执行线程上先注册一个 per-thread 计数槽（{@code ThreadID.threadID}），而业务/读
 * 线程不可控、数量可能超过 64。因此：
 * <ul>
 *   <li>构造固定 n（≤64）个写线程，每个线程启动时绑定唯一且稳定的槽下标 → 写线程与其
 *       计数器行一一对应；</li>
 *   <li>写操作提交到该池执行；若调用者本身是池内写线程（重入，例如移除监听器内再写），
 *       则<b>就地执行</b>，避免自己等自己造成的死锁；</li>
 *   <li>读路径不经由此池（无锁直读），保证读性能；</li>
 *   <li>过载保护：非池写提交须先通过有界准入闸，任务队列亦有界；拿不到闸位即视为
 *       过载，超时抛 {@link java.util.concurrent.RejectedExecutionException} 快速失败，
 *       而非无界排队/无限阻塞（防 OOM）。</li>
 * </ul>
 */
final class MutationExecutor implements AutoCloseable {

    /** 标记当前线程是否为本池写线程（实例级，避免多池串扰）。 */
    private final ThreadLocal<Boolean> onPool = ThreadLocal.withInitial(() -> Boolean.FALSE);

    /** 默认准入闸上限：同时允许"排队或执行中"的写提交总数固定为 64（内存封顶，防御 OOM）。 */
    private static final int DEFAULT_MAX_PENDING_WRITES = 64;
    /** 等待闸位的默认超时：超出视为过载，抛异常快速失败，而非无限阻塞。 */
    private static final long SUBMIT_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(5);
    /** 过载重试总次数（含首次）：偶发过载暂停后重试一次通常即恢复；耗尽仍过载则快速失败。 */
    private static final int MAX_SUBMIT_ATTEMPTS = 2;
    /** 重试前暂停：写任务微秒级，给池一个让出负载的时间窗。 */
    private static final long RETRY_PAUSE_MILLIS = 200L;
    /** 诊断日志：JUL 默认不落地，宿主自行配置；仅在写路径反复过载时输出。 */
    private static final Logger LOG = Logger.getLogger(MutationExecutor.class.getName());

    /** 同时允许"排队或执行中"的非池写提交数上限（封顶排队内存，防无界队列 OOM）。 */
    private final int maxPendingWrites;
    /** 有界准入闸：非池调用方先拿票再提交，票数 = maxPendingWrites。 */
    private final Semaphore admits;
    /** 拿票最长等待时长；超时抛 {@link RejectedExecutionException}。 */
    private final long submitTimeoutNanos;

    private final ThreadPoolExecutor pool;
    private final AtomicInteger tidSeq = new AtomicInteger(0);
    private volatile boolean closed = false;

    /**
     * @param threads 写线程数，须在 [1, {@link ThreadSlots#MAX_SLOTS}]
     */
    MutationExecutor(int threads) {
        this(threads, DEFAULT_MAX_PENDING_WRITES, SUBMIT_TIMEOUT_NANOS);
    }

    /**
     * @param threads            写线程数，须在 [1, {@link ThreadSlots#MAX_SLOTS}]
     * @param maxPendingWrites   同时允许"排队或执行中"的非池写提交数上限，须为正
     * @param submitTimeoutNanos 非池调用方等待闸位的最大时长；超时抛 {@link RejectedExecutionException}
     */
    MutationExecutor(int threads, int maxPendingWrites, long submitTimeoutNanos) {
        if (threads < 1 || threads > ThreadSlots.MAX_SLOTS) {
            throw new IllegalArgumentException("writer threads out of range: " + threads);
        }
        if (maxPendingWrites < 1) {
            throw new IllegalArgumentException("maxPendingWrites out of range: " + maxPendingWrites);
        }
        this.maxPendingWrites = maxPendingWrites;
        this.admits = new Semaphore(maxPendingWrites);
        this.submitTimeoutNanos = submitTimeoutNanos;
        ThreadFactory factory = runnable -> {
            int tid = tidSeq.getAndIncrement();
            Thread t = new Thread(() -> {
                // 绑定本线程的 SP 计数槽；此后该线程所有写操作计入 tid 行
                ThreadID.threadID.set(tid);
                onPool.set(Boolean.TRUE);
                runnable.run();
            }, "lcache-writer-" + tid);
            t.setDaemon(true);
            return t;
        };
        // 有界队列：容量 = maxPendingWrites + 1。持票提交必有空位；+1 是给"合并闸限流、最多
        // 在排 1 个"的维护任务（execute）留的空位，避免它抢不到位导致半途的 put 抛异常。
        this.pool = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(maxPendingWrites + 1), factory);
        pool.prestartAllCoreThreads();
    }

    /**
     * 同步执行写任务并等待结果。
     * <ul>
     *   <li>调用者若是本池写线程 → 就地执行（避免池满时死锁），不占用准入闸；</li>
     *   <li>否则先通过有界准入闸（最多 {@link #maxPendingWrites} 个并发写提交）。拿不到闸位
     *       视为过载：短暂暂停后<b>重试一次</b>仍失败，则打日志并抛
     *       {@link RejectedExecutionException} 快速失败，避免无界排队/无限阻塞导致的 OOM。</li>
     * </ul>
     */
    <T> T submitAndAwait(Callable<T> task) {
        ensureOpen();
        if (Boolean.TRUE.equals(onPool.get())) {
            return runInline(task);
        }
        // 有界准入 + 有限重试：写任务微秒级，偶发过载暂停后重试通常即恢复；耗尽仍过载才快速失败。
        int attempt = 0;
        while (true) {
            attempt++;
            if (tryAcquireAdmit()) {
                try {
                    return awaitOnPool(task);
                } finally {
                    admits.release();
                }
            }
            if (attempt >= MAX_SUBMIT_ATTEMPTS) {
                throw overloaded();
            }
            sleepBeforeRetry();
        }
    }

    /** 已持票：把任务交给池并阻塞等待结果（调用方保证持票，finally 负责还票）。 */
    private <T> T awaitOnPool(Callable<T> task) {
        FutureTask<T> ft = new FutureTask<>(task);
        pool.execute(ft); // 已持票 → 队列必有空位（容量 = maxPendingWrites + 1），不会因满被拒
        try {
            return ft.get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while awaiting cache write", ie);
        } catch (ExecutionException ee) {
            throw wrap(ee.getCause());
        }
    }

    /** 过载：记录诊断日志并快速失败（任务未入队、未执行，调用方可安全重试）。 */
    private RejectedExecutionException overloaded() {
        LOG.warning(() -> "cache write overloaded after " + MAX_SUBMIT_ATTEMPTS + " attempts: pending="
                + (maxPendingWrites - admits.availablePermits()) + "/" + maxPendingWrites
                + ", queue=" + pool.getQueue().size());
        return new RejectedExecutionException("cache write overloaded after "
                + MAX_SUBMIT_ATTEMPTS + " attempts");
    }

    /** 尝试在超时内获取准入闸位；被中断则恢复中断位并抛出。 */
    private boolean tryAcquireAdmit() {
        try {
            return admits.tryAcquire(submitTimeoutNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while submitting cache write", ie);
        }
    }

    /** 重试前暂停；被中断则恢复中断位并抛出。 */
    private void sleepBeforeRetry() {
        try {
            Thread.sleep(RETRY_PAUSE_MILLIS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while submitting cache write", ie);
        }
    }

    /** 池线程重入：就地执行写任务，不提交到池。 */
    private <T> T runInline(Callable<T> task) {
        try {
            return task.call();
        } catch (Exception e) {
            throw wrap(e);
        }
    }

    /** 异步执行（如周期性维护触发），调用者若是写线程则就地执行。 */
    void execute(Runnable task) {
        ensureOpen();
        if (Boolean.TRUE.equals(onPool.get())) {
            task.run();
        } else {
            pool.execute(task);
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("cache already closed");
        }
    }

    private static RuntimeException wrap(Throwable t) {
        if (t instanceof RuntimeException re) {
            return re;
        }
        if (t instanceof Error err) {
            throw err; // Error 保持抛出，不做包装
        }
        return new RuntimeException(t);
    }

    @Override
    public void close() {
        closed = true;
        pool.shutdown();
    }
}
