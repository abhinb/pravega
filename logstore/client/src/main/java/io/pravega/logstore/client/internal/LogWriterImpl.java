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
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.common.util.Retry;
import io.pravega.logstore.client.LogClientConfig;
import io.pravega.logstore.client.LogWriter;
import io.pravega.logstore.client.internal.connections.ClientConnectionFactory;
import io.pravega.logstore.shared.LogChunkExistsException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class LogWriterImpl implements LogWriter {
    @Getter
    private final long logId;
    private final LogClientConfig config;
    private final LogServerManager logServerManager;
    private final ClientConnectionFactory connectionFactory;
    private final String traceLogId;
    private final AtomicReference<ActiveChunk> activeChunk;
    private final WriteQueue writeQueue;
    private final SequentialAsyncProcessor writeProcessor;
    private final AtomicBoolean closed;
    private final AtomicLong nextChunkId;

    public LogWriterImpl(long logId, @NonNull LogClientConfig config, @NonNull LogServerManager logServerManager,
                         @NonNull ClientConnectionFactory connectionFactory) {
        this.logId = logId;
        this.config = config;
        this.logServerManager = logServerManager;
        this.connectionFactory = connectionFactory;
        this.activeChunk = new AtomicReference<>(null);
        this.writeQueue = new WriteQueue();
        val retry = createRetryPolicy(this.config.getMaxWriteAttempts(), this.config.getWriteTimeoutMillis());
        this.writeProcessor = new SequentialAsyncProcessor(this::processWritesSync, retry, this::handleWriteProcessorFailures, getExecutor());
        this.closed = new AtomicBoolean(false);
        this.traceLogId = String.format("LogWriter[%s]", this.logId);
        this.nextChunkId = new AtomicLong(0L);
    }

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.writeProcessor.close();
            val activeWriter = getActiveChunk();
            if (activeWriter != null) {
                activeWriter.writer.close();
            }

            this.writeQueue.close().forEach(w -> w.fail(new ObjectClosedException("BookKeeperLog has been closed."), true));
        }
    }

    @Override
    public CompletableFuture<Void> initialize() {
        // 0. (not done yet) Get all existing chunks and seal/fence them.
        // 1. Create new chunk.
        return rollover();
    }

    @Override
    public CompletableFuture<EntryAddress> append(@NonNull ByteBuf data) {
        val activeWriter = getActiveChunk();
        Preconditions.checkState(activeWriter != null, "Not initialized.");
        val entry = new PendingAddEntry(data, 0);
        try {
            entry.setWriter(activeWriter.writer);
            this.writeQueue.add(entry);
            this.writeProcessor.runAsync();
            entry.getCompletion()
                    .whenCompleteAsync((address, ex) -> {
                        entry.close(); // Release the buffers.
                        handleWriteException(ex);
                    }, getExecutor());

            return entry.getCompletion().thenApply(v -> new EntryAddress(entry.getChunkId(), entry.getEntryId()));
        } catch (Throwable ex) {
            entry.close(); // Release the buffers.
            throw ex;
        }
    }

    private ActiveChunk getActiveChunk() {
        return this.activeChunk.get();
    }

    private void processWritesSync() {
        if (this.closed.get()) {
            // BookKeeperLog is closed. No point in trying anything else.
            return;
        }

        if (getActiveChunk().writer.isSealed()) {
            // Current writer is closed. Execute the rollover processor to safely create a new writer. This will reinvoke
            // the write processor upon finish, so the writes can be reattempted.
            rollover();
        } else if (!processPendingWrites() && !this.closed.get()) {
            // We were not able to complete execution of all writes. Try again.
            this.writeProcessor.runAsync();
        }
    }

    private boolean processPendingWrites() {
        // Clean up the write queue of all finished writes that are complete (successfully or failed for good)
        val cleanupResult = this.writeQueue.removeFinishedWrites();
        if (cleanupResult.getStatus() == WriteQueue.CleanupStatus.WriteFailed) {
            // We encountered a failed write. As such, we must close immediately and not process anything else.
            // Closing will automatically cancel all pending writes.
            close();
            return false;
        } else {

            if (cleanupResult.getStatus() == WriteQueue.CleanupStatus.QueueEmpty) {
                // Queue is empty - nothing else to do.
                return true;
            }
        }

        // Get the writes to execute from the queue.
        List<PendingAddEntry> toExecute = getWritesToExecute();

        // Execute the writes, if any.
        boolean success = true;
        if (!toExecute.isEmpty()) {
            success = executeWrites(toExecute);

            if (success) {
                // After every run where we did write, check if need to trigger a rollover.
                rollover();
            }
        }

        return success;
    }

    /**
     * Collects an ordered list of Writes to execute to BookKeeper.
     *
     * @return The list of Writes to execute.
     */
    private List<PendingAddEntry> getWritesToExecute() {
        // Calculate how much estimated space there is in the current ledger.
        final long maxTotalSize = this.config.getRolloverSizeBytes() - getActiveChunk().writer.getLength();

        // Get the writes to execute from the queue.
        List<PendingAddEntry> toExecute = this.writeQueue.getWritesToExecute(maxTotalSize);

        // Check to see if any writes executed on closed ledgers, in which case they either need to be failed (if deemed
        // appropriate, or retried).
        if (handleClosedLedgers(toExecute)) {
            // If any changes were made to the Writes in the list, re-do the search to get a more accurate list of Writes
            // to execute (since some may have changed Ledgers, more writes may not be eligible for execution).
            toExecute = this.writeQueue.getWritesToExecute(maxTotalSize);
        }

        return toExecute;
    }

    /**
     * Executes the given Writes to BookKeeper.
     *
     * @param toExecute The Writes to execute.
     * @return True if all the writes succeeded, false if at least one failed (if a Write failed, all subsequent writes
     * will be failed as well).
     */
    private boolean executeWrites(List<PendingAddEntry> toExecute) {
        log.debug("{}: Executing {} writes.", this.traceLogId, toExecute.size());
        val activeChunk = getActiveChunk();
        for (int i = 0; i < toExecute.size(); i++) {
            PendingAddEntry entry = toExecute.get(i);
            try {
                // Record the beginning of a new attempt.
                int attemptCount = entry.beginAttempt();
                if (attemptCount > this.config.getMaxWriteAttempts()) {
                    // Retried too many times.
                    throw new RetriesExhaustedException(entry.getFailureCause());
                }

                // Invoke the BookKeeper write.
                assert entry.getChunkWriter() == activeChunk.writer;

                entry.setEntryId(activeChunk.nextEntryId.getAndIncrement());
                activeChunk.writer.addEntry(entry)
                        .whenComplete((r, ex) -> addCallback(entry, ex));
            } catch (Throwable ex) {
                // Synchronous failure (or RetriesExhausted). Fail current write.
                boolean isFinal = !isRetryable(ex);
                entry.fail(ex, isFinal);

                // And fail all remaining writes as well.
                for (int j = i + 1; j < toExecute.size(); j++) {
                    toExecute.get(j).fail(new Exception("Previous write failed.", ex), isFinal);
                }

                return false;
            }
        }

        // Success.
        return true;
    }

    private void addCallback(PendingAddEntry entry, Throwable error) {
        try {
            if (error == null) {
                assert entry != null;
                completeWrite(entry);
                return;
            }

            // Convert the response code into an Exception. Eventually this will be picked up by the WriteProcessor which
            // will retry it or fail it permanently (this includes exceptions from rollovers).
            entry.fail(error, !isRetryable(error));
        } catch (Throwable ex) {
            // Most likely a bug in our code. We still need to fail the write so we don't leave it hanging.
            entry.fail(error, !isRetryable(error));
        } finally {
            // Process all the appends in the queue after any change. This finalizes the completion, does retries (if needed)
            // and triggers more appends.
            try {
                this.writeProcessor.runAsync();
            } catch (ObjectClosedException ex) {
                // In case of failures, the WriteProcessor may already be closed. We don't want the exception to propagate
                // to BookKeeper.
                log.warn("{}: Not running WriteProcessor as part of callback due to BookKeeperLog being closed.", this.traceLogId, ex);
            }
        }
    }

    private boolean handleClosedLedgers(List<PendingAddEntry> writes) {
        if (writes.size() == 0 || !writes.get(0).getChunkWriter().isSealed()) {
            // Nothing to do. We only need to check the first write since, if a Write failed with LedgerClosed, then the
            // first write must have failed for that reason (a Ledger is closed implies all ledgers before it are closed too).
            return false;
        }

        val currentWriter = getActiveChunk();
        boolean anythingChanged = false;
        for (PendingAddEntry w : writes) {
            if (w.isDone() || !w.getChunkWriter().isSealed()) {
                continue;
            }

            // Write likely failed because of LedgerClosedException. Need to check the LastAddConfirmed for each
            // involved Ledger and see if the write actually made it through or not.
            long lac = w.getChunkWriter().getLastAckedEntryId();
            if (w.getEntryId() >= 0 && w.getEntryId() <= lac) {
                // Write was actually successful. Complete it and move on.
                completeWrite(w);
                anythingChanged = true;
            } else if (currentWriter.writer.getChunkId() != w.getChunkWriter().getChunkId()) {
                // Current ledger has changed; attempt to write to the new one.
                w.setWriter(currentWriter.writer);
                anythingChanged = true;
            }
        }

        return anythingChanged;
    }


    private CompletableFuture<Void> rollover() {
        if (this.closed.get()) {
            return CompletableFuture.completedFuture(null);
        }

        val activeChunk = this.activeChunk.get();
        if (activeChunk != null && activeChunk.writer.getLength() < this.config.getRolloverSizeBytes()) {
            // No need to rollover.
            this.writeProcessor.runAsync();
            return CompletableFuture.completedFuture(null);
        }

        log.info("{}: Rolling over.", this.traceLogId);
        return createNextChunk()
                .thenAccept(newWriter -> {
                    val currentChunk = this.activeChunk.getAndSet(new ActiveChunk(newWriter));
                    try {
                        if (currentChunk != null) {
                            currentChunk.writer.close();
                        }
                    } catch (Throwable ex) {
                        newWriter.close();
                        throw ex;
                    }
                })
                .exceptionally(this::handleRolloverFailure)
                .thenRunAsync(this.writeProcessor::runAsync, getExecutor());
    }

    private CompletableFuture<LogChunkWriter> createNextChunk() {
        log.debug("{}: Creating new chunk for log.", this.traceLogId);
        val result = new AtomicReference<LogChunkWriter>(null);
        return Futures.loop(
                () -> result.get() == null,
                () -> {
                    val chunkId = this.nextChunkId.getAndIncrement();
                    val w = new LogChunkWriterImpl(getLogId(), chunkId, this.config.getReplicationFactor(), this.logServerManager, getExecutor());
                    return w.initialize(this.connectionFactory)
                            .handle((r, ex) -> {
                                if (ex == null) {
                                    // Found one.
                                    result.set(w);
                                    log.info("{}: Created new chunk {}.", this.traceLogId, w.getChunkId());
                                } else {
                                    w.close();
                                    ex = Exceptions.unwrap(ex);
                                    if (!(ex instanceof LogChunkExistsException)) {
                                        // some other exception.
                                        throw new CompletionException(ex);
                                    }
                                }
                                return null;
                            });
                },
                getExecutor())
                .thenApply(v -> result.get());
    }

    /**
     * Completes the given Write and makes any necessary internal updates.
     *
     * @param write The write to complete.
     */
    private void completeWrite(PendingAddEntry write) {
        write.complete();
    }

    private static boolean isRetryable(Throwable ex) {
        ex = Exceptions.unwrap(ex);
        return false; // todo implement this once rollover is done.
    }

    private void handleWriteProcessorFailures(Throwable exception) {
        log.warn("{}: Too many write processor failures; closing.", this.traceLogId, exception);
        close();
    }

    private Void handleRolloverFailure(Throwable exception) {
        log.warn("{}: Too many rollover failures; closing.", this.traceLogId, exception);
        close();
        return null;
    }

    /**
     * Handles a general Write exception.
     */
    private void handleWriteException(Throwable ex) {
        if (ex instanceof ObjectClosedException && !this.closed.get()) {
            log.warn("{}: Caught ObjectClosedException but not closed; closing now.", this.traceLogId, ex);
            close();
        }
    }

    private Retry.RetryAndThrowBase<? extends Exception> createRetryPolicy(int maxWriteAttempts, int writeTimeout) {
        int initialDelay = writeTimeout / maxWriteAttempts;
        int maxDelay = writeTimeout * maxWriteAttempts;
        return Retry.withExpBackoff(initialDelay, 2, maxWriteAttempts, maxDelay)
                .retryWhen(ex -> true); // Retry for every exception.
    }

    private ScheduledExecutorService getExecutor() {
        return this.connectionFactory.getInternalExecutor();
    }

    @RequiredArgsConstructor
    private static class ActiveChunk {
        final LogChunkWriter writer;
        private final AtomicLong nextEntryId = new AtomicLong(0);
    }
}
