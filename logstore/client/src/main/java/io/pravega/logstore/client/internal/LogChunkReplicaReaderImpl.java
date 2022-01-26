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
import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.Futures;
import io.pravega.logstore.client.Entry;
import io.pravega.logstore.client.EntryAddress;
import io.pravega.logstore.client.internal.connections.ClientConnection;
import io.pravega.logstore.client.internal.connections.ClientConnectionFactory;
import io.pravega.logstore.shared.LogChunkReplicaCorruptedException;
import io.pravega.logstore.shared.protocol.ReplyProcessor;
import io.pravega.logstore.shared.protocol.commands.ChunkInfo;
import io.pravega.logstore.shared.protocol.commands.EntriesRead;
import io.pravega.logstore.shared.protocol.commands.EntryData;
import io.pravega.logstore.shared.protocol.commands.GetChunkInfo;
import io.pravega.logstore.shared.protocol.commands.ReadEntries;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class LogChunkReplicaReaderImpl implements LogChunkReader {
    @Getter
    private final long chunkId;
    private final URI logStoreUri;
    private final Executor executor;
    private final String traceLogId;
    private final State state;
    private final ReplyProcessor responseProcessor;

    public LogChunkReplicaReaderImpl(long logId, long chunkId, @NonNull URI logStoreUri, @NonNull Executor executor) {
        this.chunkId = chunkId;
        this.logStoreUri = logStoreUri;
        this.executor = executor;
        this.traceLogId = String.format("ChunkReader[%s-%s-%s:%s]", logId, chunkId, logStoreUri.getHost(), logStoreUri.getPort());
        this.responseProcessor = new ResponseProcessor(this.traceLogId, this::failAndClose);
        this.state = new State();
    }

    @Override
    public CompletableFuture<Void> initialize(@NonNull ClientConnectionFactory connectionFactory) {
        Preconditions.checkState(this.state.connection.get() == null, "Already initialized.");
        log.info("Intializing logchunkreplicareaderImpl");
        try {
            connectionFactory.establishConnection(this.logStoreUri, this.responseProcessor)
                    .thenAccept(connection -> {
                        this.state.setConnection(connection);
                        getChunkInfo();
                    })
                    .whenComplete((r, ex) -> {
                        if (ex != null) {
                            failAndClose(ex);
                        }
                    });
            return state.initialized;
        } catch (Throwable ex) {
            failAndClose(ex);
            return Futures.failedFuture(ex);
        }
    }

    @Override
    public long getEntryCount() {
        ensureInitialized();
        log.info("intialized ...getting entrycount");
        return this.state.getEntryCount();
    }

    @Override
    public long getLength() {
        ensureInitialized();
        return this.state.getLength();
    }

    @Override
    public Collection<URI> getReplicaURIs() {
        return Collections.singleton(this.logStoreUri);
    }

    @Override
    public void close() {
        val c = this.state;
        if (c != null) {
            c.connection.get().close();
            c.initialized.completeExceptionally(new ObjectClosedException(this));
            log.info("{}: Closed.", this.traceLogId);
        }
    }

    private void failAndClose(Throwable ex) {
        log.error("{}: Error.", this.traceLogId, ex);
        state.fail(ex);
        close();
    }

    @Override
    public CompletableFuture<List<Entry>> getNext() {
        ensureInitialized();
        return this.state.readNext();
    }

    private void ensureInitialized() {
        Preconditions.checkState(this.state.connection.get() != null, "Not initialized.");
    }

    @Override
    public String toString() {
        return String.format("%s: %s/%s (%s bytes).", this.traceLogId, this.state.getNextEntryId(), this.state.getEntryCount(), this.state.getLength());
    }

    @SneakyThrows
    private void getChunkInfo() {
        this.state.getConnection().send(new GetChunkInfo(0L, this.chunkId));
    }

    @RequiredArgsConstructor
    private class State {
        final AtomicReference<ClientConnection> connection = new AtomicReference<>();
        final AtomicReference<ChunkInfo> chunkInfo = new AtomicReference<>();
        final CompletableFuture<Void> initialized = new CompletableFuture<>();
        final AtomicReference<CompletableFuture<List<Entry>>> pendingRead = new AtomicReference<>();
        private final AtomicLong nextEntryId = new AtomicLong();

        void setConnection(ClientConnection connection) {
            this.connection.set(connection);
        }

        ClientConnection getConnection() {
            return this.connection.get();
        }

        long getNextEntryId() {
            return this.nextEntryId.get();
        }

        long getEntryCount() {
            val ci = this.chunkInfo.get();
            return ci == null ? -1L : ci.getEntryCount();
        }

        long getLength() {
            val ci = this.chunkInfo.get();
            return ci == null ? -1L : ci.getLength();
        }

        CompletableFuture<List<Entry>> readNext() {
            log.info("LogChunkReplicaReader: readNext called");
            if (this.nextEntryId.get() >= getEntryCount()) {
                // End of the chunk.
                log.info("in readnext..closing logchunkreader...nextentryid {} greater than entrycount {}",this.nextEntryId.get(), getEntryCount());
                close();
                return CompletableFuture.completedFuture(null);
            }

            val result = new CompletableFuture<List<Entry>>();
            Preconditions.checkState(this.pendingRead.compareAndSet(null, result), "Another read is in progress.");

            try {
                val request = new ReadEntries(0L, chunkId, getNextEntryId());
                log.info("Read entrries sent for chunk {} and next entryid {} ",chunkId, getNextEntryId());
                getConnection().send(request);
            } catch (Throwable ex) {
                this.pendingRead.compareAndSet(result, null);
                result.completeExceptionally(ex);
            }

            return result;
        }

        private void completeRead(List<Entry> entries) {
            val result = this.pendingRead.get();
            assert result != null && !result.isDone();
            try {
                for (val e : entries) {
                    if (e.getAddress().getEntryId() != getNextEntryId()) {
                        failAndClose(new LogChunkReplicaCorruptedException(chunkId, "Read Entries out of order. Expected Entry Id %s, Found Entry Id %s.", this.nextEntryId, e.getAddress().getEntryId()));
                        return;
                    }

                    this.nextEntryId.incrementAndGet();
                }

                this.pendingRead.compareAndSet(result, null);
                log.info("completing read ");
                result.complete(entries);
            } finally {
                this.pendingRead.compareAndSet(result, null);
            }
        }

        void fail(Throwable ex) {
            val pendingRead = this.pendingRead.getAndSet(null);
            if (pendingRead != null) {
                pendingRead.completeExceptionally(ex);
            }
        }
    }

    private class ResponseProcessor extends ReplyProcessorBase {
        ResponseProcessor(String traceLogId, Consumer<Throwable> errorCallback) {
            super(traceLogId, errorCallback);
        }

        @Override
        public void chunkInfo(ChunkInfo chunkInfo) {
            state.chunkInfo.set(chunkInfo);
            state.initialized.complete(null);
            log.info("{}: EntryCount={}, Length={}.", traceLogId, chunkInfo.getEntryCount(), chunkInfo.getLength());
        }

        @Override
        public void entriesRead(EntriesRead entriesRead) {
            try {
                List<Entry> entries = entriesRead.getEntries().stream()
                        .map(e -> new ReadEntry(e, chunkId))
                        .collect(Collectors.toList());
                state.completeRead(entries);
            } finally {
                entriesRead.release();
            }
        }
    }

    @Getter
    private static class ReadEntry implements Entry {
        private final EntryAddress address;
        private final int crc32;
        private final int length;
        private final ByteBuf data;

        ReadEntry(EntryData e, long chunkId) {
            this.address = new EntryAddress(chunkId, e.getEntryId());
            this.crc32 = e.getCrc32();
            this.data = e.getData().copy();
            this.length = this.data.readableBytes();
        }
    }
}
