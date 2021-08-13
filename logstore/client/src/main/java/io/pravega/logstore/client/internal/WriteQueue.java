/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.logstore.client.internal;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@ThreadSafe
class WriteQueue {
    //region Members

    @Getter
    private final Supplier<Long> timeSupplier;
    @GuardedBy("this")
    private final Deque<PendingAddEntry> writes;
    @GuardedBy("this")
    private long totalLength;
    @GuardedBy("this")
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the WriteQueue class.
     */
    WriteQueue() {
        this(System::nanoTime);
    }

    /**
     * Creates a new instance of the WriteQueue class.
     *
     * @param timeSupplier A Supplier that returns the current time, in nanoseconds.
     */
    @VisibleForTesting
    WriteQueue(Supplier<Long> timeSupplier) {
        this.timeSupplier = Preconditions.checkNotNull(timeSupplier, "timeSupplier");
        this.writes = new ArrayDeque<>();
    }

    //endregion

    //region Queue Operations

    /**
     * Adds a new Write to the end of the queue.
     *
     * @param entry The write to add.
     */
    synchronized void add(PendingAddEntry entry) {
        Exceptions.checkNotClosed(this.closed, this);
        this.writes.addLast(entry);
        this.totalLength += entry.getLength();
        entry.setStartTime(this.timeSupplier.get());
    }

    /**
     * Clears the queue of all the items and closes it, preventing any new writes from being added.
     *
     * @return A new List with the contents of the queue (prior to cleanup), in the same order.
     */
    synchronized List<PendingAddEntry> close() {
        List<PendingAddEntry> items = new ArrayList<>(this.writes);
        this.writes.clear();
        this.totalLength = 0;
        this.closed = true;
        return items;
    }

    /**
     * Gets an ordered List of Writes that are ready to be executed. The returned writes are not removed from the queue
     * and are in the same order they are in the queue. They are not necessarily the first items in the queue (if, for
     * example, the head of the queue has a bunch of completed Writes).
     * This method will return writes as long as:
     * * The MaxSize limit is not reached
     * * The writes to return have the same Ledger Id assigned as the first write in the queue.
     *
     * @param maximumAccumulatedSize The maximum total accumulated size of the items to return. Once this value is exceeded,
     *                               no further writes are returned.
     * @return The result.
     */
    synchronized List<PendingAddEntry> getWritesToExecute(long maximumAccumulatedSize) {
        Exceptions.checkNotClosed(this.closed, this);
        long accumulatedSize = 0;

        // Collect all remaining writes, as long as they are not currently in-progress and have the same ledger id
        // as the first item in the ledger.
        long firstLedgerId = this.writes.peekFirst().getChunkWriter().getChunkId();
        boolean canSkip = true;

        List<PendingAddEntry> result = new ArrayList<>();
        for (PendingAddEntry write : this.writes) {
            if (accumulatedSize >= maximumAccumulatedSize) {
                // Either reached the throttling limit or ledger max size limit.
                // If we try to send too many writes to this ledger, the writes are likely to be rejected with
                // LedgerClosedException and simply be retried again.
                break;
            }

            // Account for this write's size, even if it's complete or in progress.
            accumulatedSize += write.getLength();
            if (write.isInProgress()) {
                if (!canSkip) {
                    // We stumbled across an in-progress write after a not-in-progress write. We can't retry now.
                    // This is likely due to a bunch of writes failing (i.e. due to a LedgerClosedEx), but we overlapped
                    // with their updating their status. Try again next time (when that write completes).
                    return Collections.emptyList();
                }
            } else if (write.getChunkWriter().getChunkId() != firstLedgerId) {
                // We cannot initiate writes in a new ledger until all writes in the previous ledger completed.
                break;
            } else if (!write.isDone()) {
                canSkip = false;
                result.add(write);
            }
        }

        return result;
    }

    /**
     * Removes all the completed writes (whether successful or failed) from the beginning of the queue, until the first
     * non-completed item is encountered or the queue is empty.
     *
     * @return A CleanupResult representing the result of the Operation. If there were failed writes, {@link CleanupResult#getStatus()}
     * will be {@link CleanupStatus#WriteFailed), otherwise it will be one of {@link CleanupStatus#QueueEmpty} or
     * {@link CleanupStatus#QueueNotEmpty}, depending on the final state of the queue when this method finishes.
     */
    synchronized CleanupResult removeFinishedWrites() {
        Exceptions.checkNotClosed(this.closed, this);
        long currentTime = this.timeSupplier.get();
        long totalElapsed = 0;
        int removedCount = 0;
        boolean failedWrite = false;
        while (!this.writes.isEmpty() && this.writes.peekFirst().isDone()) {
            PendingAddEntry w = this.writes.removeFirst();
            this.totalLength = Math.max(0, this.totalLength - w.getLength());
            removedCount++;
            totalElapsed += currentTime - w.getStartTime();
            failedWrite |= w.getFailureCause() != null;
        }

        CleanupStatus status = failedWrite
                ? CleanupStatus.WriteFailed
                : this.writes.isEmpty() ? CleanupStatus.QueueEmpty : CleanupStatus.QueueNotEmpty;
        return new CleanupResult(status, removedCount);
    }

    //endregion

    //region CleanupResult

    /**
     * The result of a call to {@link #removeFinishedWrites()}.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    static class CleanupResult {
        /**
         * The final status of the queue.
         */
        private final CleanupStatus status;
        /**
         * The number of removed writes.
         */
        private final int removedCount;
    }

    //endregion

    //region CleanupStatus

    /**
     * Defines various states that the WriteQueue may be in after a cleanup is performed.
     */
    enum CleanupStatus {
        /**
         * The Queue is empty after the operation.
         */
        QueueEmpty,

        /**
         * The Queue is not empty after the operation.
         */
        QueueNotEmpty,

        /**
         * A permanently failed Write was detected.
         */
        WriteFailed
    }

    //endregion
}

