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
import io.pravega.common.ObjectBuilder;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.common.util.Retry;
import io.pravega.logstore.client.LogClientConfig;
import io.pravega.logstore.client.LogInfo;
import io.pravega.logstore.client.LogWriter;
import io.pravega.logstore.client.QueueStatistics;
import io.pravega.logstore.client.internal.connections.ClientConnectionFactory;
import io.pravega.logstore.shared.LogChunkExistsException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Builder;
import lombok.Data;
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
    private final String metadataPath;

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
        this.metadataPath = String.format("/%s/%s", LogMetadata.PATH, logId);
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
        val m = this.metadata.get();
        return new LogInfo(this.logId, this.metadata.get().getChunks().size(), m.getEpoch());
    }

    @Override
    public CompletableFuture<Void> initialize() {
        // 1. Fetch metadata.
        this.metadata.set(this.metadataManager.get(this.metadataPath, LogMetadata.SERIALIZER::deserialize, LogMetadata.empty()));
        updateMetadata(this.metadata.get().newEpoch());

        // 2. (TBD - not done yet) Get all existing chunks and seal/fence them.

        // 3. Create new chunk.
        return rollover();
    }

    @Override
    public CompletableFuture<EntryAddress> append(@NonNull ByteBuf data) {
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
                .thenAccept(newWriter -> {
                    val currentChunk = this.activeChunk.getAndSet(new WriteChunk(newWriter));
                    try {
                        if (currentChunk != null) {
                            currentChunk.markRolledOver();
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
                    val chunkId = this.metadataManager.getNextChunkId();
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
        if (this.metadataManager.set(m, this.metadataPath, LogMetadata.SERIALIZER::serialize)) {
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

    @Data
    private static class ChunkMetadata {
        private final long chunkId;
        private final Collection<URI> locations;
    }

    @Getter
    @Builder
    private static class LogMetadata extends VersionedMetadata {
        static final String PATH = "LogMetadata";
        private static final Serializer SERIALIZER = new Serializer();
        private final long epoch;
        private final List<ChunkMetadata> chunks;

        static LogMetadata empty() {
            return new LogMetadata(0L, Collections.emptyList());
        }

        LogMetadata addChunk(long chunkId, Collection<URI> locations) {
            val newChunks = new ArrayList<>(this.chunks);
            newChunks.add(new ChunkMetadata(chunkId, locations));
            val result = new LogMetadata(this.epoch, newChunks);
            result.setVersion(this.getVersion());
            return result;
        }

        LogMetadata newEpoch() {
            return new LogMetadata(this.epoch + 1, this.chunks);
        }

        public static class LogMetadataBuilder implements ObjectBuilder<LogMetadata> {

        }

        private static class Serializer extends VersionedSerializer.WithBuilder<LogMetadata, LogMetadataBuilder> {
            @Override
            protected LogMetadataBuilder newBuilder() {
                return builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(LogMetadata e, RevisionDataOutput target) throws IOException {
                target.writeLong(e.epoch);
                target.writeCollection(e.chunks, this::writeChunk00);
            }

            private void read00(RevisionDataInput source, LogMetadataBuilder b) throws IOException {
                b.epoch(source.readLong());
                b.chunks(source.readCollection(this::readChunk00, ArrayList::new));
            }

            private void writeChunk00(RevisionDataOutput target, ChunkMetadata c) throws IOException {
                target.writeLong(c.chunkId);
                target.writeCollection(c.locations, (t, uri) -> t.writeUTF(uri.toString()));
            }

            private ChunkMetadata readChunk00(RevisionDataInput source) throws IOException {
                val chunkId = source.readLong();
                val locations = source.readCollection(s -> URI.create(s.readUTF()), ArrayList::new);
                return new ChunkMetadata(chunkId, locations);
            }
        }
    }
}
