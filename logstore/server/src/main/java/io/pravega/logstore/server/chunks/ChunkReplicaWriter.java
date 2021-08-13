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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BlockingDrainingQueue;
import io.pravega.logstore.server.ChunkEntry;
import io.pravega.logstore.server.LogStoreConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class ChunkReplicaWriter implements AutoCloseable {
    private static final IndexFormat INDEX_FORMAT = new IndexFormat();
    private final LogStoreConfig config;
    private final AppendOnlyFileWriter dataWriter;
    private final AppendOnlyFileWriter indexWriter;
    private final BlockingDrainingQueue<PendingWrite> writeQueue;
    private final String traceLogId;
    private final AtomicBoolean closed;
    private final AtomicReference<CompletableFuture<Void>> runner;
    private final ScheduledExecutorService executorService;
    @Setter
    private volatile Runnable onClose;

    ChunkReplicaWriter(long chunkId, @NonNull LogStoreConfig config, @NonNull ScheduledExecutorService executorService) {
        this.config = config;
        this.traceLogId = String.format("ReplicaWriter[%s]", chunkId);
        this.dataWriter = new AppendOnlyFileWriter(config.getChunkReplicaDataFilePath(chunkId), true);
        this.indexWriter = new AppendOnlyFileWriter(config.getChunkReplicaIndexFilePath(chunkId), false);
        this.writeQueue = new BlockingDrainingQueue<>();
        this.closed = new AtomicBoolean(false);
        this.executorService = executorService;
        this.runner = new AtomicReference<>();
    }

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            // Close the queue and cancel everything caught in flight.
            val pendingWrites = this.writeQueue.close();
            pendingWrites.forEach(PendingWrite::cancel);
            try {
                this.dataWriter.close(); // Auto-flush.
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

            log.info("Closed.");
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
                () -> this.writeQueue.take(this.config.getMaxQueueReadCount())
                        .thenAcceptAsync(this::processWrites, this.executorService),
                this.executorService)
                .exceptionally(this::handleError));
    }

    public CompletableFuture<Long> append(@NonNull ChunkEntry entry) {
        Preconditions.checkState(this.runner.get() != null, "Not started.");
        val write = new PendingWrite(entry);
        log.debug("{}: append {}.", this.traceLogId, entry);
        this.writeQueue.add(write); // this will throw ObjectClosedException if we are closed.
        return write.getCompletion();
    }

    private void processWrites(Queue<PendingWrite> writes) {
        log.debug("{}: processWrites (Count = {}).", this.traceLogId, writes.size());
        WriteBatch batch = newWriteBatch(this.dataWriter.getLength());
        try {
            while (!writes.isEmpty()) {
                val write = writes.poll();
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
                    ex = Exceptions.unwrap(ex);
                    cancelIncompleteWrites(Collections.singleton(write), ex);
                    throw ex;
                }

                if (writes.isEmpty()) { // TODO yield after a long time to give others a chance to use the pool.
                    // Check if there are more operations to process. If so, do it now.
                    writes = this.writeQueue.poll(this.config.getMaxQueueReadCount());
                }
            }

            // There may be leftover writes in the batch. Flush them too.
            flushBatch(batch);
        } catch (Throwable ex) {
            ex = Exceptions.unwrap(ex);

            // Fail all writes we picked from the writeQueue and hold within our temp queue.
            cancelIncompleteWrites(writes, ex);

            // Fail all writes we picked from both the writeQueue and temp queue and are pending in a WriteBatch.
            // Note this will only "fail" those that haven't yet been acked.
            cancelIncompleteWrites(batch.getWrites(), ex);

            // Rethrow the exception - this will cause everything to shut down and cancel anything left in the queue.
            throw Exceptions.sneakyThrow(ex);
        }
    }

    private void flushBatch(WriteBatch writeBatch) throws IOException {
        // 1. Flush to data file. This call syncs to disk.
        this.dataWriter.append(writeBatch.get(), writeBatch.getOffset());
        log.debug("{}: Flushed {}.", this.traceLogId, writeBatch);

        // 2. Ack writes.
        writeBatch.complete();
        log.trace("{}: Acked {}.", this.traceLogId, writeBatch);

        // 3. Write index.
        val indexWrite = INDEX_FORMAT.serialize(writeBatch.getWrites());
        this.indexWriter.append(indexWrite, this.indexWriter.getLength());
        log.trace("{}: Wrote Index for {}.", this.traceLogId, writeBatch);
    }

    private WriteBatch newWriteBatch(long offset) {
        return new WriteBatch(offset, this.config.getWriteBlockSize());
    }

    private void cancelIncompleteWrites(Iterable<PendingWrite> operations, Throwable failException) {
        assert failException != null : "no exception to set";
        int cancelCount = 0;
        for (PendingWrite o : operations) {
            o.fail(failException);
            cancelCount++;
        }

        log.warn("{}: Cancelling {} writes with exception: {}.", this.traceLogId, cancelCount, failException.toString());
    }

    @SneakyThrows
    private Void handleError(Throwable ex) {
        log.error("{}: Processing error. Closing.", this.traceLogId, ex);
        close();
        return null;
    }

    @Getter
    private static class WriteBatch {
        private final long offset;
        private int remainingCapacity;
        private int length;
        private final ArrayList<ByteBuf> buffers = new ArrayList<>();
        private final ArrayList<PendingWrite> writes = new ArrayList<>();

        WriteBatch(long offset, int maxSize) {
            this.offset = offset;
            this.remainingCapacity = maxSize;
            this.length = 0;
        }

        @Override
        public String toString() {
            return String.format("Offset=%s, Length=%s, EntryIds=[%s-%s]", this.offset, this.length, getFirstEntryId(), getLastEntryId());
        }

        Long getFirstEntryId() {
            return this.writes.isEmpty() ? null : this.writes.get(0).getEntryId();
        }

        Long getLastEntryId() {
            return this.writes.isEmpty() ? null : this.writes.get(this.writes.size() - 1).getEntryId();
        }

        boolean isFull() {
            return this.remainingCapacity <= 0;
        }

        ByteBuf get() {
            return Unpooled.wrappedUnmodifiableBuffer(this.buffers.toArray(new ByteBuf[this.buffers.size()]));
        }

        boolean add(PendingWrite write) {
            if (isFull()) {
                return false;
            }

            ByteBuf buf;
            if (write.getData().readableBytes() <= this.remainingCapacity) {
                // Whole fit
                buf = write.getData();
                this.writes.add(write);
            } else {
                // Partial fit
                buf = write.getData().slice(0, this.remainingCapacity);
            }

            this.buffers.add(buf);
            this.remainingCapacity -= buf.readableBytes();
            this.length += buf.readableBytes();
            write.slice(buf.readableBytes());
            return !write.hasData();
        }

        void complete() {
            this.writes.forEach(PendingWrite::complete);
        }
    }
}
