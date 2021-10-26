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
package io.pravega.logstore.server.chunks;

import com.google.common.base.Preconditions;
import io.netty.buffer.Unpooled;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.SequentialAsyncProcessor;
import io.pravega.common.function.Callbacks;
import io.pravega.common.util.BlockingDrainingQueue;
import io.pravega.common.util.Retry;
import io.pravega.logstore.server.ChunkEntry;
import io.pravega.logstore.server.LogStoreConfig;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.annotation.concurrent.GuardedBy;
import lombok.NonNull;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class ChunkReplicaWriter implements AutoCloseable {
    private static final DataFormat DATA_FORMAT = new DataFormat();
    private static final IndexFormat INDEX_FORMAT = new IndexFormat();
    private static final Retry.RetryAndThrowConditionally FLUSH_RETRY = Retry.withExpBackoff(5, 2, 10, 1000)
            .retryWhen(ex -> true); // Retry for every exception.

    private final long chunkId;
    private final LogStoreConfig config;
    private final AppendOnlyFileWriter dataWriter;
    private final AppendOnlyFileWriter indexWriter;
    private final BlockingDrainingQueue<PendingChunkOp> opQueue;
    @GuardedBy("ackQueue")
    private final Deque<ChunkEntry> ackQueue;
    private final String traceLogId;
    private final AtomicBoolean closed;
    private final AtomicReference<CompletableFuture<Void>> runner;
    private final ScheduledExecutorService executorService;
    private final AtomicLong entryCount;
    @Setter
    private volatile Runnable onClose;
    private volatile boolean stopped;
    @GuardedBy("ackQueue")
    private long nextExpectedEntryId;
    @Setter
    private volatile Consumer<WriteCompletion> completionCallback;
    private final AtomicLong lastWrittenEntryId;
    private final SequentialAsyncProcessor dataFlusher;

    ChunkReplicaWriter(long chunkId, @NonNull LogStoreConfig config, @NonNull ScheduledExecutorService executorService) {
        this.chunkId = chunkId;
        this.config = config;
        this.traceLogId = String.format("ReplicaWriter[%s]", chunkId);
        this.dataWriter = new AppendOnlyFileWriter(config.getChunkReplicaDataFilePath(chunkId));
        this.indexWriter = new AppendOnlyFileWriter(config.getChunkReplicaIndexFilePath(chunkId));
        this.dataFlusher = new SequentialAsyncProcessor(this::flushData, FLUSH_RETRY, this::flushError, executorService);
        this.opQueue = new BlockingDrainingQueue<>();
        this.ackQueue = new ArrayDeque<>();
        this.entryCount = new AtomicLong(0);
        this.lastWrittenEntryId = new AtomicLong(-1L);
        this.nextExpectedEntryId = 0L;
        this.closed = new AtomicBoolean(false);
        this.executorService = executorService;
        this.runner = new AtomicReference<>();
        this.stopped = false;
    }

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            // Close the queue and cancel everything caught in flight.
            val pendingOps = this.opQueue.close();
            cancelIncompleteOperations(pendingOps, new CancellationException());
            this.dataFlusher.close(); // No more auto-flushes.
            try {
                // Flush everything there is to flush.
                flushData();
                this.dataWriter.close(); // Auto-flush (rendundant, but ensures every single byte is sync-ed).
                log.info("{}: Data File Writer flushed and closed with length {}.", this.traceLogId, this.dataWriter.getLength());
            } catch (IOException ex) {
                log.error("{}: Unable to close Data File Writer.", this.traceLogId, ex);
            }
            try {
                this.indexWriter.close(); // Auto-flush.
                log.info("{}: Index File Writer flushed and closed with length {}.", this.traceLogId, this.indexWriter.getLength());
            } catch (IOException ex) {
                log.error("{}: Unable to close Index File Writer.", this.traceLogId, ex);
            }

            // Write metadata.
            try (val metadataWriter = new AppendOnlyFileWriter(config.getChunkReplicaMetadataFilePath(this.chunkId))) {
                metadataWriter.open();
                val metadata = new ChunkMetadata(this.chunkId, this.entryCount.get(), this.dataWriter.getLength(), this.indexWriter.getLength());
                metadataWriter.append(Unpooled.wrappedBuffer(ChunkMetadata.SERIALIZER.serialize(metadata).getCopy()), 0L);
                metadataWriter.flush();
            } catch (Throwable ex) {
                log.error("{}: Unable to write metadata.", this.traceLogId, ex);
            }

            log.info("{}: Closed.", this.traceLogId);
            val toRun = this.onClose;
            if (toRun != null) {
                this.executorService.execute(toRun);
            }
        }
    }

    public void start() throws IOException {
        Preconditions.checkState(this.runner.get() == null, "Already started.");

        this.dataWriter.open();
        this.indexWriter.open();

        this.runner.set(Futures.loop(
                () -> !this.closed.get(),
                () -> this.opQueue.take(this.config.getMaxQueueReadCount())
                        .thenAcceptAsync(this::processOperations, this.executorService),
                this.executorService)
                .exceptionally(this::handleError));
    }

    public CompletableFuture<Void> stopAndClose() {
        Preconditions.checkState(this.runner.get() != null, "Not started.");
        val seal = new PendingChunkSeal();
        log.debug("{}: Seal.", this.traceLogId);
        this.opQueue.add(seal); // this will throw ObjectClosedException if we are closed.
        return Futures.toVoid(seal.getCompletion()).thenRun(this::close);
    }

    public void append(@NonNull ChunkEntry entry) {
        Preconditions.checkState(this.runner.get() != null, "Not started.");
        val write = new PendingChunkWrite(entry, DATA_FORMAT);
        synchronized (this.ackQueue) {
            Preconditions.checkArgument(entry.getEntryId() == this.nextExpectedEntryId, "Expected Entry Id %s, given %s.", this.nextExpectedEntryId, entry.getEntryId());
            log.debug("{}: append {}.", this.traceLogId, entry);
            this.opQueue.add(write); // this will throw ObjectClosedException if we are closed.
            this.ackQueue.addLast(entry);
            this.nextExpectedEntryId++;
        }
    }

    private void processOperations(Queue<PendingChunkOp> operations) {
        log.debug("{}: processOperations (Count = {}).", this.traceLogId, operations.size());
        WriteBatch batch = newWriteBatch(this.dataWriter.getLength());
        try {
            while (!operations.isEmpty()) {
                val op = operations.poll();
                if (op.isTerminal() || this.stopped) {
                    processTerminalOperation((PendingChunkSeal) op, batch, operations);
                    break;
                }

                batch = processWriteOperation((PendingChunkWrite) op, batch, operations);
                if (operations.isEmpty()) { // TODO yield after a long time to give others a chance to use the pool.
                    // Check if there are more operations to process. If so, do it now.
                    operations = this.opQueue.poll(this.config.getMaxQueueReadCount());
                }
            }

            // There may be leftover writes in the batch. Flush them too.
            flushBatch(batch);
        } catch (Throwable ex) {
            ex = Exceptions.unwrap(ex);

            // Fail all writes we picked from the writeQueue and hold within our temp queue.
            cancelIncompleteOperations(operations, ex);

            // Fail all writes we picked from both the writeQueue and temp queue and are pending in a WriteBatch.
            // Note this will only "fail" those that haven't yet been acked.
            cancelIncompleteOperations(batch.getWrites(), ex);

            // Rethrow the exception - this will cause everything to shut down and cancel anything left in the queue.
            throw Exceptions.sneakyThrow(ex);
        }
    }

    private void processTerminalOperation(PendingChunkSeal seal, WriteBatch batch, Iterable<PendingChunkOp> pendingOps) throws IOException {
        // Last write - flush what we have and close.
        this.stopped = true;
        try {
            flushBatch(batch);
            cancelIncompleteOperations(pendingOps, new CancellationException());
            seal.complete();
        } catch (Throwable ex) {
            // We must cancel this operation as we've already picked it from the
            cancelIncompleteOperations(Collections.singleton(seal), Exceptions.unwrap(ex));
            throw ex;
        }
    }

    private WriteBatch processWriteOperation(PendingChunkWrite write, WriteBatch batch, Iterable<PendingChunkOp> pendingChunkOps) throws IOException {
        try {
            // Assign offset.
            write.setOffset(batch.getOffset() + batch.getLength());

            // The write may span multiple batches. Write it, piece by piece, to the batch and create new batches if needed.
            while (write.hasData()) {
                batch.add(write);
                if (batch.isFull()) {
                    flushBatch(batch);
                    long newOffset = batch.getOffset() + batch.getLength();
                    batch = newWriteBatch(newOffset);
                }
            }
        } catch (Throwable ex) {
            // We must cancel this operation as we've already picked it from the
            cancelIncompleteOperations(Collections.singleton(write), Exceptions.unwrap(ex));
            throw ex;
        }
        return batch;
    }

    private WriteBatch newWriteBatch(long offset) {
        int length = this.config.getWriteBlockSize() - (int) (offset % this.config.getWriteBlockSize());
        return new WriteBatch(offset, length);
    }

    private void flushBatch(WriteBatch writeBatch) throws IOException {
        if (writeBatch.isEmpty()) {
            // Nothing to do.
            return;
        }

        // 1. Write to data file. This does not sync to disk yet.
        this.dataWriter.append(writeBatch.get(), writeBatch.getOffset());

        // 2. Invoke the flusher (which will run async). When it's done, it will ack whatever it flushed.
        if (writeBatch.hasCompletedEntries()) {
            this.lastWrittenEntryId.set(writeBatch.getLastEntryId());
            log.debug("{}: Wrote {}.", this.traceLogId, writeBatch);

            this.dataFlusher.runAsync();
            this.entryCount.addAndGet(writeBatch.getWrites().size());

            // 3. Write index.
            val indexWrite = INDEX_FORMAT.serialize(writeBatch.getWrites());
            this.indexWriter.append(indexWrite, this.indexWriter.getLength());
            log.trace("{}: Wrote Index for {}.", this.traceLogId, writeBatch);
        }
    }

    @SneakyThrows
    private void flushData() {
        final long lastEntryId = this.lastWrittenEntryId.get();
        this.dataWriter.flush();

        // 2. Ack writes.
        val entries = getEntriesToAckUntilId(lastEntryId); // These are already returned in correct order.
        if (entries.size() > 0) {
            val wc = new WriteCompletion(this.chunkId, entries, null);
            Callbacks.invokeSafely(this.completionCallback, wc, this::ackError);
            log.debug("{}: Flushed and acked {}.", this.traceLogId, wc);
        }

        if (this.lastWrittenEntryId.get() != lastEntryId) {
            // We have new data to flush; do it again.
            try {
                this.dataFlusher.runAsync();
            } catch (ObjectClosedException ex) {
                log.warn("{}: Not re-running data flusher due to shutting down.", this.traceLogId);
            }
        }
    }

    private <T extends PendingChunkOp> void cancelIncompleteOperations(Iterable<T> operations, Throwable failException) {
        assert failException != null : "no exception to set";
        val entries = new ArrayList<ChunkEntry>();
        for (PendingChunkOp o : operations) {
            if (o.hasCompletion()) {
                o.fail(failException);
            } else {
                assert o instanceof PendingChunkWrite;
                entries.addAll(getEntriesToAckFromId(((PendingChunkWrite) o).getEntryId()));
            }
        }

        if (entries.size() > 0) {
            entries.sort(Comparator.comparingLong(ChunkEntry::getEntryId)); // These are out of order (reversed/shuffled) so we must sort.
            val wc = new WriteCompletion(this.chunkId, entries, failException);
            Callbacks.invokeSafely(this.completionCallback, wc, this::ackError);
            log.warn("{}: Cancelled {} with exception: {}.", this.traceLogId, wc, failException.toString());
        }
    }

    private List<ChunkEntry> getEntriesToAckUntilId(long entryId) {
        val result = new ArrayList<ChunkEntry>();
        synchronized (this.ackQueue) {
            while (!this.ackQueue.isEmpty() && this.ackQueue.peekFirst().getEntryId() <= entryId) {
                result.add(this.ackQueue.removeFirst());
            }
        }
        return result;
    }

    private List<ChunkEntry> getEntriesToAckFromId(long entryId) {
        val result = new ArrayList<ChunkEntry>();
        synchronized (this.ackQueue) {
            while (!this.ackQueue.isEmpty() && this.ackQueue.peekLast().getEntryId() >= entryId) {
                result.add(this.ackQueue.removeLast());
            }
        }
        return result;
    }

    @SneakyThrows
    private Void handleError(Throwable ex) {
        ex = Exceptions.unwrap(ex);
        if (!(ex instanceof CancellationException || ex instanceof ObjectClosedException)) {
            log.error("{}: Processing error. Closing.", this.traceLogId, ex);
        }
        close();
        return null;
    }

    private void flushError(Throwable ex) {
        log.error("{}: Disk flush failure. Closing.", this.traceLogId, ex);
        close();
    }

    private void ackError(Throwable ex) {
        log.warn("{}: Ack failure.", this.traceLogId, ex);
    }

    @FunctionalInterface
    public interface TriConsumer<T, U, V> {
        void accept(T var1, U var2, V var3);
    }

}
