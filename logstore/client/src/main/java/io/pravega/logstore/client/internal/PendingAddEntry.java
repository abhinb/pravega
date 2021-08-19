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

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.pravega.common.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
public class PendingAddEntry implements Entry, AutoCloseable {
    private final int crc32;
    @NonNull
    private final ByteBuf data;
    private final int length;
    private final CompletableFuture<Void> completion;
    private final AtomicInteger attemptCount;
    private final AtomicReference<Timer> beginAttemptTimer;
    private final AtomicReference<Throwable> failureCause;
    private volatile WriteChunk writeChunk;
    private volatile long entryId;
    @Setter
    private volatile long startTime;

    public PendingAddEntry(@NonNull ByteBuf data, int crc32) {
        this.data = data;
        this.crc32 = crc32;
        this.attemptCount = new AtomicInteger();
        this.beginAttemptTimer = new AtomicReference<>();
        this.failureCause = new AtomicReference<>();
        this.length = data.readableBytes();
        this.completion = new CompletableFuture<>();
        this.writeChunk = null;
        this.entryId = Long.MIN_VALUE;
    }

    public void setWriter(WriteChunk wc) {
        this.writeChunk = wc;
        this.entryId = Long.MIN_VALUE;
    }

    public void assignEntryId() {
        this.entryId = this.writeChunk.getNextEntryId();
    }

    /**
     * Records the fact that a new attempt to execute this write is begun.
     *
     * @return The current attempt number.
     */
    int beginAttempt() {
        Preconditions.checkState(this.beginAttemptTimer.compareAndSet(null, new Timer()),
                "Entry already in progress. Cannot restart.");
        return this.attemptCount.incrementAndGet();
    }

    /**
     * Records the fact that an attempt to execute this write has ended.
     */
    private Timer endAttempt() {
        return this.beginAttemptTimer.getAndSet(null);
    }

    /**
     * Gets a value indicating whether an attempt to execute this write is in progress.
     *
     * @return True or false.
     */
    boolean isInProgress() {
        return this.beginAttemptTimer.get() != null;
    }

    /**
     * Gets a value indicating whether this write is completed (successfully or not).
     *
     * @return True or false.
     */
    boolean isDone() {
        return this.completion.isDone();
    }

    /**
     * Gets the failure cause, if any.
     *
     * @return The failure cause.
     */
    Throwable getFailureCause() {
        return this.failureCause.get();
    }

    /**
     * Indicates that this write completed successfully. This will set the final result on the externalCompletion future.
     */
    Timer complete() {
        this.failureCause.set(null);
        this.completion.complete(null);
        return endAttempt();
    }

    /**
     * Indicates that this write failed.
     *
     * @param cause    The failure cause. If null, the previous failure cause is preserved.
     * @param complete If true, the externalCompletion will be immediately be completed with the current failure cause.
     *                 If false, no completion will be done.
     */
    void fail(Throwable cause, boolean complete) {
        if (cause != null) {
            this.failureCause.set(cause);
        }

        endAttempt();
        WriteChunk chunk = this.writeChunk;
        if (chunk != null && chunk.isSealed()) {
            // Rollovers aren't really failures (they're caused by us). In that case, do not count this failure as an attempt.
            this.attemptCount.updateAndGet(v -> Math.max(0, v - 1));
        }

        if (complete) {
            this.completion.completeExceptionally(this.failureCause.get());
        }
    }

    @Override
    public String toString() {
        return String.format("LogChunkId=%s, EntryId=%s, Crc=%s, Length=%s, Status=%s", this.writeChunk, this.entryId, this.crc32,
                this.length, this.completion.isDone() ? (this.completion.isCompletedExceptionally() ? "Error" : "Done") : "Pending");
    }

    @Override
    public void close() {
        this.data.release();
    }

    @Override
    public EntryAddress getAddress() {
        return new EntryAddress(this.writeChunk.getWriter().getChunkId(), getEntryId());
    }
}
