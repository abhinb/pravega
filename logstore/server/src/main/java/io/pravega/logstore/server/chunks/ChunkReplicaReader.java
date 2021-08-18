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
import io.pravega.common.Exceptions;
import io.pravega.common.MathHelpers;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.Futures;
import io.pravega.logstore.server.ChunkEntry;
import io.pravega.logstore.server.ChunkInfo;
import io.pravega.logstore.server.LogStoreConfig;
import io.pravega.logstore.shared.LogChunkReplicaCorruptedException;
import io.pravega.logstore.shared.protocol.EnhancedByteBufInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class ChunkReplicaReader implements AutoCloseable {
    private static final DataFormat DATA_FORMAT = new DataFormat();
    private static final IndexFormat INDEX_FORMAT = new IndexFormat();
    private final long chunkId;
    private final LogStoreConfig config;
    private final ScheduledExecutorService executorService;
    private final String traceLogId;
    private final AtomicBoolean closed;
    private final AtomicReference<CompletableFuture<Void>> initialized;
    private volatile ChunkMetadata chunkMetadata;
    private volatile FileReader dataReader;
    private volatile FileReader indexReader;

    ChunkReplicaReader(long chunkId, @NonNull LogStoreConfig config, @NonNull ScheduledExecutorService executorService) {
        this.chunkId = chunkId;
        this.config = config;
        this.executorService = executorService;
        this.traceLogId = String.format("ReplicaReader[%s]", chunkId);
        this.closed = new AtomicBoolean();
        this.initialized = new AtomicReference<>();
    }

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            close(this.dataReader, "Data File Reader");
            close(this.indexReader, "Index File Reader");
            val i = this.initialized.get();
            if (i != null) {
                i.completeExceptionally(new ObjectClosedException(this));
            }
        }
    }

    private void close(AutoCloseable c, String name) {
        try {
            if (c != null) {
                c.close();
            }
        } catch (Throwable ex) {
            log.error("{}: Unable to close {}.", this.traceLogId, name, ex);
        }
    }

    //region Initialization

    public CompletableFuture<Void> ensureInitialized() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        if (this.initialized.compareAndSet(null, result)) {
            // First time
            Futures.completeAfter(
                    () -> CompletableFuture.runAsync(this::initialize, this.executorService),
                    result);
            Futures.exceptionListener(result, ex -> {
                log.error("{}: Initialization failure.", this.traceLogId, ex);
                close();
            });
        } else {
            // Subsequent times.
            result = this.initialized.get();
        }
        return result;
    }

    @SneakyThrows(IOException.class)
    private void initialize() {
        Exceptions.checkNotClosed(this.closed.get(), this);

        // Read metadata. If this doesn't exist or is corrupted, we can't do anything else.
        loadMetadata();
        assert this.chunkMetadata != null;

        try {
            // Open Index and Data files. There is no recovery done here as part of this prototype. Everything must be in place.
            this.dataReader = new FileReader(this.config.getChunkReplicaDataFilePath(this.chunkId));
            this.indexReader = new FileReader(this.config.getChunkReplicaIndexFilePath(this.chunkId));
            this.dataReader.open();
            this.indexReader.open();
        } catch (Throwable ex) {
            throw new LogChunkReplicaCorruptedException(this.chunkId, ex.toString());
        }

        validateChunkReplicaFiles();
    }

    private void validateChunkReplicaFiles() {
        if (this.dataReader.getLength() != this.chunkMetadata.getDataLength()) {
            throw new LogChunkReplicaCorruptedException(this.chunkId, "Data File Length mismatch. Expected %s, found %s.",
                    this.chunkMetadata.getDataLength(), this.dataReader.getLength());
        }

        if (this.indexReader.getLength() != this.chunkMetadata.getIndexLength()) {
            throw new LogChunkReplicaCorruptedException(this.chunkId, "Index File Length mismatch. Expected %s, found %s.",
                    this.chunkMetadata.getIndexLength(), this.indexReader.getLength());
        }
    }

    private void loadMetadata() throws IOException, LogChunkReplicaCorruptedException {
        try (val reader = new FileReader(this.config.getChunkReplicaMetadataFilePath(this.chunkId))) {
            reader.open();
            if (reader.getLength() > ChunkMetadata.Serializer.MAX_SERIALIZATION_SIZE) {
                throw new LogChunkReplicaCorruptedException(chunkId, "Metadata file too long (%s).", reader.getLength());
            }
            val buf = reader.read(0, (int) reader.getLength());
            this.chunkMetadata = ChunkMetadata.SERIALIZER.deserialize(new EnhancedByteBufInputStream(buf));
        }
    }

    //endregion

    public CompletableFuture<ChunkInfo> getInfo() {
        return ensureInitialized().thenApply(v -> this.chunkMetadata);
    }

    /**
     * Reads at least one entry beginning with the given entry Id, but not exceeding the total number of bytes specified.
     * If the maxSizeBytes is less than the size of the first entry, then one entry will be returned.
     *
     * @param entryId      Entry Id.
     * @param maxSizeBytes Max Size bytes.
     * @return A future.
     */
    public CompletableFuture<List<ChunkEntry>> read(long entryId, int maxSizeBytes) {
        return ensureInitialized()
                .thenApplyAsync(v -> readFromEntryId(entryId, maxSizeBytes), this.executorService);
    }

    @SneakyThrows(IOException.class)
    private List<ChunkEntry> readFromEntryId(long entryId, int maxSizeBytes) {
        Preconditions.checkArgument(entryId >= 0 && entryId < this.chunkMetadata.getEntryCount(), "Invalid Entry Id %s.", entryId);
        maxSizeBytes = MathHelpers.minMax(maxSizeBytes, 0, this.config.getMaxReadSize());
        log.debug("{}: readFromEntryId entryId={}, maxSizeBytes={}.", this.traceLogId, entryId, maxSizeBytes);
        long startOffset = findOffset(entryId);
        if (startOffset < 0 || startOffset >= this.chunkMetadata.getDataLength()) {
            throw new LogChunkReplicaCorruptedException(this.chunkId, "Invalid offset (%s) mapped to Entry Id (%s).", startOffset, entryId);
        }

        // We can't read beyond the end of the file.
        maxSizeBytes = (int) Math.min(maxSizeBytes, this.dataReader.getLength() - startOffset);
        ByteBuf buf = this.dataReader.read(startOffset, maxSizeBytes);

        // Deserialize as many entries as we can from this buffer.
        val result = new ArrayList<ChunkEntry>();
        deserializeEntries(buf, result);

        if (result.isEmpty()) {
            if (maxSizeBytes >= this.config.getMaxReadSize() || startOffset + maxSizeBytes >= this.dataReader.getLength()) {
                log.warn("{}: Unable to read any entry since current entry exceeds max allowed read size ({}).", this.traceLogId, this.config.getMaxReadSize());
                return result;
            } else {
                log.debug("{}: Unable to read any entry with maxSizeBytes={}; trying with max allowed value of {}.", this.traceLogId, maxSizeBytes, this.config.getMaxReadSize());
                maxSizeBytes = (int) Math.min(this.config.getMaxReadSize(), this.dataReader.getLength() - startOffset);
                buf = this.dataReader.read(startOffset, maxSizeBytes);
                deserializeEntries(buf, result);
            }
        }

        return result;
    }

    private void deserializeEntries(ByteBuf buf, List<ChunkEntry> result) {
        ChunkEntry e;
        do {
            e = DATA_FORMAT.deserialize(buf, this.chunkId);
            if (e != null) {
                result.add(e);
            }
        } while (e != null);
    }

    private long findOffset(long entryId) throws IOException {
        long indexFileOffset = 0;
        while (indexFileOffset < this.chunkMetadata.getIndexLength()) {
            int readLength = (int) Math.min(this.config.getReadBlockSize(), this.chunkMetadata.getIndexLength() - indexFileOffset);
            val buf = this.indexReader.read(0, readLength);
            long soughtOffset = findOffset(entryId, buf);
            if (soughtOffset >= 0) {
                return soughtOffset;
            }

            // Advance offset but account for the fact that we may not have been able to read the whole buffer.
            indexFileOffset += readLength - buf.readableBytes();
        }
        return -1L;
    }

    private long findOffset(long entryId, ByteBuf buf) {
        do {
            val e = INDEX_FORMAT.deserialize(buf);
            if (e == null) {
                break;
            } else if (e.getEntryId() == entryId) {
                return e.getOffset();
            }
        } while (buf.readableBytes() > 0);
        return -1L;
    }
}
