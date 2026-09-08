package algorithms.size.core;

/**
 *  This is an implementation of the paper "Concurrent Size" by Gal Sela and Erez Petrank.
 *  The current file is the core of the size methodology.
 *
 *  Copyright (C) 2022  Gal Sela
 *  Contact Gal Sela (sela.galy@gmail.com) with any questions or comments.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

import measurements.support.ThreadID;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class SizeCalculator {
    private static final int PADDING = 16; // This is for 128-bytes padding: PADDING*sizeof(long)
    private final long[][] metadataCounters = new long[ThreadID.MAX_THREADS + 1][PADDING]; // The '+1' is for padding before the array, to prevent false sharing with thread 0
    private volatile CountersSnapshot countersSnapshot = new CountersSnapshot().deactivate();

    private final ThreadLocal<Backoff> backoff = ThreadLocal.withInitial(Backoff::new);

    public long compute() {
        // Obtain collecting CountersSnapshot:
        CountersSnapshot activeCountersSnapshot;
        boolean didEncounterConcurrentSize = false;
        CountersSnapshot currentCountersSnapshot = (CountersSnapshot) COUNTERS_SNAPSHOT.getVolatile(this);
        if (currentCountersSnapshot.isCollecting()) {
            activeCountersSnapshot = currentCountersSnapshot;
            didEncounterConcurrentSize = true;
        }
        else {
            CountersSnapshot newCountersSnapshot = new CountersSnapshot();
            CountersSnapshot witnessedCountersSnapshot = (CountersSnapshot) COUNTERS_SNAPSHOT.compareAndExchange(
                    this, currentCountersSnapshot, newCountersSnapshot);
            if (witnessedCountersSnapshot == currentCountersSnapshot) {
                activeCountersSnapshot = newCountersSnapshot;
            }
            else {
                // Our exchange failed, adopt the CountersSnapshot written by a concurrent thread
                activeCountersSnapshot = witnessedCountersSnapshot;
                didEncounterConcurrentSize = true;
            }
        }
        if (didEncounterConcurrentSize) {
            backoff.get().backoff();
            long currentSize = activeCountersSnapshot.retrieveSize();
            if (currentSize != CountersSnapshot.INVALID_SIZE) {
                return currentSize;
            }
        }

        collect(activeCountersSnapshot);
        activeCountersSnapshot.deactivate(); // This is size's linearization point
        return activeCountersSnapshot.computeSize(backoff.get());
    }

    private void collect(CountersSnapshot targetCountersSnapshot) {
        for (int tid = 0; tid < ThreadID.MAX_THREADS; ++tid) {
            for (int opKind = 0; opKind < UpdateOperations.OPS_NUM; ++opKind) {
                targetCountersSnapshot.add(tid, opKind, getThreadUpdateCounter(tid, opKind));
            }
        }
    }

    public void updateMetadata(int opKind, UpdateInfoHolder updateInfoHolder) {
        int tid = updateInfoHolder.getTid();
        long newCounter = updateInfoHolder.getCounter();

        if (getThreadUpdateCounter(tid, opKind) == newCounter - 1) {
            METADATA_COUNTERS.compareAndSet(metadataCounters[tid + 1], opKind, newCounter - 1, newCounter);
        }

        CountersSnapshot currentCountersSnapshot = (CountersSnapshot) COUNTERS_SNAPSHOT.getVolatile(this);
        if (currentCountersSnapshot.isCollecting() && getThreadUpdateCounter(tid, opKind) == newCounter) {
            currentCountersSnapshot.forward(tid, opKind, newCounter);
        }
    }

    public UpdateInfo createUpdateInfo(int opKind) {
        int tid = ThreadID.threadID.get();
        return new UpdateInfo(tid, getThreadUpdateCounter(tid, opKind) + 1);
    }

    public long getThreadUpdateCounter(int tid, int opKind) {
        return (long) METADATA_COUNTERS.getVolatile(metadataCounters[tid + 1], opKind);
    }

    private static final VarHandle METADATA_COUNTERS = MethodHandles.arrayElementVarHandle(long[].class);

    private static final VarHandle COUNTERS_SNAPSHOT;
    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            COUNTERS_SNAPSHOT = l.findVarHandle(
                    SizeCalculator.class, "countersSnapshot", CountersSnapshot.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // MODIFICATION (2026-07-28): 新增 — 用于 CROQ 崩溃恢复时直接设置线程计数器的初始值。
    // 此方法仅在单线程恢复阶段使用，不经过 createUpdateInfo/updateMetadata 协议。
    public void setThreadUpdateCounter(int tid, int opKind, long counter) {
        METADATA_COUNTERS.setVolatile(metadataCounters[tid + 1], opKind, counter);
    }

    private static class CountersSnapshot {
        private final long[][] snapshot = new long[ThreadID.MAX_THREADS][UpdateOperations.OPS_NUM];
        private volatile boolean collecting;
        private volatile long size;

        private static final long INVALID_COUNTER = Long.MAX_VALUE;
        private static final long INVALID_SIZE = Long.MAX_VALUE;

        public CountersSnapshot() {
            for (int tid = 0; tid < ThreadID.MAX_THREADS; ++tid) {
                for (int opKind = 0; opKind < UpdateOperations.OPS_NUM; ++opKind) {
                    SNAPSHOT.setVolatile(this.snapshot[tid], opKind, INVALID_COUNTER);
                }
            }

            COLLECTING.setVolatile(this, true);
            SIZE.setVolatile(this, INVALID_SIZE);
        }

        public void add(int tid, int opKind, long counter) {
            if (getThreadSnapshotUpdateCounter(tid, opKind) == INVALID_COUNTER) {
                SNAPSHOT.compareAndSet(snapshot[tid], opKind, INVALID_COUNTER, counter);
            }
        }

        public void forward(int tid, int opKind, long counter) {
            long snapshotCounter = getThreadSnapshotUpdateCounter(tid, opKind);
            while (snapshotCounter == INVALID_COUNTER || counter > snapshotCounter) { // shall not execute more than 2 iterations
                long witnessedSnapshotCounter = (long) SNAPSHOT.compareAndExchange(snapshot[tid], opKind, snapshotCounter, counter);
                if (witnessedSnapshotCounter == snapshotCounter) {
                    break;
                }
                snapshotCounter = witnessedSnapshotCounter;
            }
        }

        public boolean isCollecting() {
            return (boolean) COLLECTING.getVolatile(this);
        }

        public CountersSnapshot deactivate() {
            COLLECTING.setVolatile(this, false);
            return this;
        }

        public long retrieveSize() {
            return (long) SIZE.getOpaque(this);
        }

        public long computeSize(Backoff backoff) {
            long currentSize = retrieveSize();
            if (currentSize != INVALID_SIZE) {
                backoff.increase();
                return currentSize;
            }

            long computedSize = 0;
            for (int tid = 0; tid < ThreadID.MAX_THREADS; ++tid) {
                computedSize += getThreadSnapshotUpdateCounter(tid, UpdateOperations.OpKind.INSERT) -
                        getThreadSnapshotUpdateCounter(tid, UpdateOperations.OpKind.REMOVE);
            }

            currentSize = retrieveSize();
            if (currentSize != INVALID_SIZE) {
                backoff.increase();
                return currentSize;
            }

            long witnessedSize = (long) SIZE.compareAndExchange(this, INVALID_SIZE, computedSize);

            if (witnessedSize == INVALID_SIZE) {
                backoff.decrease();
                return computedSize;
            }
            // Our exchange failed, return the size written by a concurrent thread
            backoff.increase();
            return witnessedSize;
        }

        private long getThreadSnapshotUpdateCounter(int tid, int opKind) {
            return (long) SNAPSHOT.getVolatile(snapshot[tid], opKind);
        }

        private static final VarHandle SNAPSHOT = MethodHandles.arrayElementVarHandle(long[].class);

        private static final VarHandle COLLECTING;
        private static final VarHandle SIZE;
        static {
            try {
                MethodHandles.Lookup l = MethodHandles.lookup();
                COLLECTING = l.findVarHandle(CountersSnapshot.class, "collecting", boolean.class);
                SIZE = l.findVarHandle(CountersSnapshot.class, "size", long.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
    }
}
