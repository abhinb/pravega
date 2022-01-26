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
 */package io.pravega.logstore.client.internal;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.SequentialAsyncProcessor;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.common.util.Retry;
import io.pravega.logstore.client.EntryAddress;
import io.pravega.logstore.client.LogClientConfig;
import io.pravega.logstore.client.LogInfo;
import io.pravega.logstore.client.LogReader;
import io.pravega.logstore.client.LogWriter;
import io.pravega.logstore.client.QueueStatistics;
import io.pravega.logstore.client.internal.connections.ClientConnectionFactory;
import io.pravega.logstore.shared.LogChunkExistsException;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class LogWriterImpl implements LogWriter {
    @Getter
    private final long logId;
    private final LogClientConfig config;
    private final LogServerManager logServerManager;
    private final MetadataManager metadataManager;
    private final ClientConnectionFactory connectionFactory;
    private final String traceLogId;
    private final AtomicReference<WriteChunk> activeChunk;
    private final WriteQueue writeQueue;
    private final SequentialAsyncProcessor writeProcessor;
    private final AtomicBoolean closed;
    private final AtomicReference<LogMetadata> metadata;

    public LogWriterImpl(long logId, @NonNull LogClientConfig config, @NonNull LogServerManager logServerManager,
                         @NonNull MetadataManager metadataManager, @NonNull ClientConnectionFactory connectionFactory) {
        this.logId = logId;
        this.config = config;
        this.metadataManager = metadataManager;
        this.logServerManager = logServerManager;
        this.connectionFactory = connectionFactory;
        this.activeChunk = new AtomicReference<>(null);
        this.metadata = new AtomicReference<>();
        this.writeQueue = new WriteQueue();
        val retry = createRetryPolicy(this.config.getMaxWriteAttempts(), this.config.getWriteTimeoutMillis());
        this.writeProcessor = new SequentialAsyncProcessor(this::processWritesSync, retry, this::handleWriteProcessorFailures, getExecutor());
        this.closed = new AtomicBoolean(false);
        this.traceLogId = String.format("LogWriter[%s]", this.logId);
    }

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.writeProcessor.close();
            val activeWriter = getActiveChunk();
            if (activeWriter != null) {
                activeWriter.getWriter().close();
            }

            this.writeQueue.close().forEach(w -> w.fail(new ObjectClosedException(this), true));
        }
    }

    @Override
    public LogInfo getInfo() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        val m = this.metadata.get();
        return new LogInfo(this.logId, m.getChunks().size(), m.getEpoch());
    }

    @Override
    public CompletableFuture<Void> initialize() {
        Exceptions.checkNotClosed(this.closed.get(), this);

        // 1. Fetch metadata.
        this.metadata.set(this.metadataManager.get(LogMetadata.getPath(this.logId), LogMetadata.SERIALIZER::deserialize, LogMetadata.empty(this.logId)));
        updateMetadata(this.metadata.get().newEpoch());

        // 2. (TBD - not done yet) Get all existing chunks and seal/fence them.
        // There are commands for it available (SealChunk).
        sealLastChunk();

        // 3. Create new chunk.
        return rollover();
    }

    private CompletableFuture<Void> sealLastChunk() {
        ListIterator<LogMetadata.ChunkMetadata> iterator = this.metadata.get().getChunks().listIterator(this.metadata.get().getChunks().size());
        if (!iterator.hasPrevious()) {
            log.info("No ending chunk found for log {}. Log Empty!", this.getLogId());
            return null;
        }
        long chunkId = iterator.previous().getChunkId();
        val w = new LogChunkWriterImpl(getLogId(), chunkId, this.config.getReplicationFactor(), this.logServerManager, getExecutor());
        w.initWithConnection(this.connectionFactory)
         .handle((r, ex) -> {
             if (ex == null) {
                 try {
                     log.info("Sealing chunk {} on log {}", chunkId, getLogId());
                     w.seal();
                 } catch (Exception e) {
                     log.error("Error while sealing chunk {} on log {} during init. Ex {}", chunkId, this.getLogId(), ex);
                     w.close();
                     throw new CompletionException(e);
                 }
             } else {
                 log.error("Error while initializing writer for log {} while sealing last chunk {}. Ex {}", this.getLogId(), chunkId, ex);
                 w.close();
                 throw new CompletionException(ex);
             }
             log.info("close was called here . Returning null in seallastchunk");
//             w.close(); // close this writer. was only used to seal. Writer will be initialized as part of rolling over.
             return null;
         });
        return null;
    }

    @Override
    public CompletableFuture<EntryAddress> append(@NonNull ByteBuf data) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        val activeWriter = getActiveChunk();
        Preconditions.checkState(activeWriter != null, "Not initialized.");
        val entry = new PendingAddEntry(data, 0);
        try {
            entry.setWriter(activeWriter);
            this.writeQueue.add(entry);
            this.writeProcessor.runAsync();
            entry.getCompletion()
                    .whenCompleteAsync((address, ex) -> {
                        entry.close(); // Release the buffers.
                        handleWriteException(ex);
                    }, getExecutor());

            return entry.getCompletion().thenApply(v -> entry.getAddress());
        } catch (Throwable ex) {
            entry.close(); // Release the buffers.
            throw ex;
        }
    }

    @Override
    public QueueStatistics getQueueStatistics() {
        return this.writeQueue.getStatistics();
    }

    @Override
    public LogReader getReader() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(getActiveChunk() != null, "Not initialized.");
        return new LogReaderImpl(this.metadata.get(), this.config, this.connectionFactory);
    }

    private WriteChunk getActiveChunk() {
        return this.activeChunk.get();
    }

    private void processWritesSync() {
        if (this.closed.get()) {
            // BookKeeperLog is closed. No point in trying anything else.
            return;
        }

        if (getActiveChunk().getWriter().isSealed()) {
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
        final long maxTotalSize = this.config.getRolloverSizeBytes() - getActiveChunk().getWriter().getLength();

        // Get the writes to execute from the queue.
        List<PendingAddEntry> toExecute = this.writeQueue.getWritesToExecute(maxTotalSize);

        // Check to see if any writes executed on closed ledgers, in which case they either need to be failed (if deemed
        // appropriate, or retried).
        if (handleClosedChunks(toExecute)) {
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
                entry.assignEntryId();
                entry.getWriteChunk().getWriter().addEntry(entry)
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

    private boolean handleClosedChunks(List<PendingAddEntry> writes) {
        if (writes.size() == 0 || !writes.get(0).getWriteChunk().getWriter().isSealed()) {
            // Nothing to do. We only need to check the first write since, if a Write failed with LedgerClosed, then the
            // first write must have failed for that reason (a Ledger is closed implies all ledgers before it are closed too).
            return false;
        }

        val currentWriter = getActiveChunk();
        boolean anythingChanged = false;
        for (PendingAddEntry w : writes) {
            if (w.isDone() || !w.getWriteChunk().getWriter().isSealed()) {
                continue;
            }

            // Write likely failed because of LedgerClosedException. Need to check the LastAddConfirmed for each
            // involved Ledger and see if the write actually made it through or not.
            long lac = w.getWriteChunk().getWriter().getLastAckedEntryId();
            if (w.getEntryId() >= 0 && w.getEntryId() <= lac) {
                // Write was actually successful. Complete it and move on.
                completeWrite(w);
                anythingChanged = true;
            } else if (currentWriter.getWriter().getChunkId() != w.getWriteChunk().getWriter().getChunkId()) {
                // Current ledger has changed; attempt to write to the new one.
                w.setWriter(currentWriter);
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
        if (activeChunk != null && activeChunk.getWriter().getLength() < this.config.getRolloverSizeBytes()) {
            // No need to rollover.
            this.writeProcessor.runAsync();
            return CompletableFuture.completedFuture(null);
        }

        log.info("{}: Rolling over.", this.traceLogId);
        return createNextChunk()
                .thenComposeAsync(newWriter -> {
                    val currentChunk = this.activeChunk.getAndSet(new WriteChunk(newWriter));
                    if( currentChunk == null ) log.info("previous chunk is null");
                    if (currentChunk != null) {
                        log.info("previous chunk is {} and newly created chunk is {} on log {}",currentChunk.getWriter().getChunkId(), newWriter.getChunkId(), getLogId() );
                        try {
                            return currentChunk.seal();
                        } catch (Throwable ex) {
                            newWriter.close();
                            throw ex;
                        }
                    }
                    printLog(newWriter);
                    return CompletableFuture.completedFuture(null);
                }, getExecutor())
                .exceptionally(this::handleRolloverFailure)
                .thenRunAsync(this.writeProcessor::runAsync, getExecutor());
    }

    private void printLog(LogChunkWriter writer){
        StringBuilder builder = new StringBuilder();
        this.metadata.get().getChunks().forEach( meta -> builder.append(meta.getChunkId()).append(","));
        log.info("this log {} has chunks {} and the new one created is {}",getLogId(), builder.toString(), writer.getChunkId());
    }

    private CompletableFuture<LogChunkWriter> createNextChunk() {

        val result = new AtomicReference<LogChunkWriter>(null);
        return Futures.loop(
                () -> result.get() == null,
                () -> {
                    val chunkId = this.metadataManager.getNextChunkId();
                    log.debug("{}: Creating new chunk {} for log {}.", this.traceLogId, chunkId,getLogId());
                    val w = new LogChunkWriterImpl(getLogId(), chunkId, this.config.getReplicationFactor(), this.logServerManager, getExecutor());
                    return w.initialize(this.connectionFactory)
                            .handle((r, ex) -> {
                                if (ex == null) {
                                    // Found one.
                                    result.set(w);
                                    val serverURIs = w.getReplicaURIs();
                                    log.info("{}: Created chunk {} on servers {}.", this.traceLogId, w.getChunkId(), serverURIs);

                                    // Persist metadata in ZK
                                    updateMetadata(this.metadata.get().addChunk(w.getChunkId(), serverURIs));
                                } else {
                                    w.close();
                                    ex = Exceptions.unwrap(ex);
                                    if (ex instanceof LogChunkExistsException) {
                                        log.warn("{}: Chunk {} already exists on server but not in ZK. Skipping.", this.traceLogId, chunkId);
                                    } else {
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

    private void updateMetadata(LogMetadata m) {
        if (this.metadataManager.set(m, m.getPath(), LogMetadata.SERIALIZER::serialize)) {
            this.metadata.set(m);
        } else {
            throw new CompletionException(new Exception("Unable to persist metadata in ZK."));
        }
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
        return ex instanceof CancellationException
                || ex instanceof ObjectClosedException
                || ex instanceof LogChunkSealedException;
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
}
