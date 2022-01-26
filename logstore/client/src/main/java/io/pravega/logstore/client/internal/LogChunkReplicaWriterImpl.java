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
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.logstore.client.Entry;
import io.pravega.logstore.client.internal.connections.ClientConnection;
import io.pravega.logstore.client.internal.connections.ClientConnectionFactory;
import io.pravega.logstore.shared.protocol.commands.AppendEntry;
import io.pravega.logstore.shared.protocol.commands.ChunkCreated;
import io.pravega.logstore.shared.protocol.commands.ChunkSealed;
import io.pravega.logstore.shared.protocol.commands.CreateChunk;
import io.pravega.logstore.shared.protocol.commands.EntryAppended;
import io.pravega.logstore.shared.protocol.commands.EntryData;
import io.pravega.logstore.shared.protocol.commands.SealChunk;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class LogChunkReplicaWriterImpl implements LogChunkWriter {
    @Getter
    private final long chunkId;
    private final URI logStoreUri;
    private final Executor executor;
    private final Object lock = new Object();
    private final String traceLogId;
    private final AtomicLong length = new AtomicLong(0);
    private final CompletableFuture<Void> initialized;
    private volatile ClientConnection connection;
    @Getter
    private volatile boolean closed = false;
    @GuardedBy("lock")
    private final ArrayDeque<PendingChunkReplicaEntry> pending = new ArrayDeque<>();
    @GuardedBy("lock")
    private long nextExpectedEntryId = 0;
    @GuardedBy("lock")
    private long lastAckedEntryId;
    @GuardedBy("lock")
    private CompletableFuture<Void> sealed;

    public LogChunkReplicaWriterImpl(long logId, long chunkId, @NonNull URI logStoreUri,
                                     @NonNull Executor executor) {
        this.chunkId = chunkId;
        this.logStoreUri = logStoreUri;
        this.executor = executor;
        this.initialized = new CompletableFuture<>();
        this.traceLogId = String.format("ChunkWriter[%s-%s-%s:%s]", logId, chunkId, logStoreUri.getHost(), logStoreUri.getPort());
    }

    @Override
    public CompletableFuture<Void> initialize(@NonNull ClientConnectionFactory connectionFactory) {
        val responseProcessor = new ResponseProcessor(this.traceLogId, this::failAndClose);
        try {
            connectionFactory.establishConnection(logStoreUri, responseProcessor)
                    .thenAccept(connection -> {
                        this.connection = connection;
                        createLogChunk();
                    })
                    .whenComplete((r, ex) -> {
                        if (ex != null) {
                            failAndClose(ex);
                        }
                    });
            return this.initialized;
        } catch (Throwable ex) {
            failAndClose(ex);
            return Futures.failedFuture(ex);
        }
    }

    public CompletableFuture<Void> initWithConnection(@NonNull ClientConnectionFactory connectionFactory) {
        val responseProcessor = new ResponseProcessor(this.traceLogId, this::failAndClose);
        try {
            connectionFactory.establishConnection(logStoreUri, responseProcessor)
                             .thenAccept(connection -> {
                                 this.connection = connection;
                             })
                             .whenComplete((r, ex) -> {
                                 if (ex != null) {
                                     log.error("Error establishing connection {}",ex);
                                     close();
                                     throw new CompletionException(ex);
                                 }
                                 log.info("Intialized replica writer");
                                 this.initialized.complete(null);
                             });

            return this.initialized;
        } catch (Throwable ex) {
            close();
            return Futures.failedFuture(ex);
        }
    }


    @Override
    public long getLastAckedEntryId() {
        synchronized (lock) {
            return this.lastAckedEntryId;
        }
    }

    @Override
    public long getLength() {
        return this.length.get();
    }

    @Override
    public boolean isSealed() {
        return this.closed;
    }

    @Override
    public Collection<URI> getReplicaURIs() {
        return Collections.singleton(this.logStoreUri);
    }

    @SneakyThrows
    private void createLogChunk() {
        val cc = new CreateChunk(0L, this.chunkId);
        this.connection.send(cc);
    }

    @Override
    public CompletableFuture<Void> addEntry(Entry entry) {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(isInitialized(), "Not initialized.");
        Preconditions.checkArgument(entry.getAddress().getChunkId() == this.chunkId);

        synchronized (this.lock) {
            try {
                if (this.sealed != null) {
                    throw new LogChunkSealedException(this.chunkId);
                }
                Preconditions.checkArgument(entry.getAddress().getEntryId() == this.nextExpectedEntryId,
                        "Unexpected EntryId. Expected %s, given %s.", this.nextExpectedEntryId, entry.getAddress().getEntryId());

                // We only send if not already sealed.
                val entryData = new EntryData(entry.getAddress().getEntryId(), entry.getCrc32(), entry.getData());
                val ae = new AppendEntry(entry.getAddress().getChunkId(), entryData);
                log.trace("{}: Sending AppendEntry: {}", this, ae);
                this.connection.send(ae);

                val pe = new PendingChunkReplicaEntry(entry);
                this.pending.addLast(pe);
                this.nextExpectedEntryId++;
                this.length.addAndGet(entry.getLength());
                return pe.completion;
            } catch (Throwable ex) {
                log.error("{}: Failed to add new entry {}. ", this.traceLogId, entry, ex);
                return Futures.failedFuture(ex);
            }
        }
    }

    @Override
    public CompletableFuture<Void> seal() {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(isInitialized(), "Not initialized.");
        log.info("in logchunkreplicawriter:  sealing current chunk {}",this.chunkId);
        synchronized (this.lock) {
            if (this.sealed != null) {
                return this.sealed;
            }
            try {
                this.sealed = new CompletableFuture<>();
                val connection = this.connection;
                val seal = new SealChunk(0L, this.chunkId);
                log.info("{}: Sending SealChunk: {}", this, seal);
                connection.send(seal);
                return this.sealed;
            } catch (Throwable ex) {
                log.error("{}: Failed to seal.", this.traceLogId, ex);
                return Futures.failedFuture(ex);
            }
        }
    }

    @Override
    public void close() {
        if (!this.closed) {
            fail(new CancellationException("Closing."));
            val connection = this.connection;
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void failAndClose(Throwable ex) {
        fail(ex);
        close();
    }

    @Override
    public String toString() {
        return this.traceLogId;
    }

    private boolean isInitialized() {
        return this.initialized.isDone();
    }

    /**
     * @param ex Error that has occurred that needs to be handled by tearing down the connection.
     */
    private void fail(Throwable ex) {
        log.debug("{}: Handling exception {}.", traceLogId, ex.toString());
        this.closed = true;
        List<PendingChunkReplicaEntry> pending;
        CompletableFuture<Void> sealCallback;
        synchronized (lock) {
            pending = new ArrayList<>(this.pending);
            this.pending.clear();
            sealCallback = this.sealed;
        }

        executor.execute(() -> {
            this.initialized.completeExceptionally(ex);
            for (PendingChunkReplicaEntry toAck : pending) {
                toAck.completion.completeExceptionally(ex);
            }

            if (sealCallback != null) {
                sealCallback.completeExceptionally(ex);
            }
        });
    }

    private void ackUpTo(long lastAckedEntryId) {
        final List<PendingChunkReplicaEntry> pendingEntries = new ArrayList<>();
        CompletableFuture<Void> sealCallback;
        synchronized (lock) {
            PendingChunkReplicaEntry entry = pending.peekFirst();
            while (entry != null && entry.entry.getAddress().getEntryId() <= lastAckedEntryId) {
                pending.pollFirst();
                pendingEntries.add(entry);
                entry = pending.peekFirst();
            }

            this.lastAckedEntryId = lastAckedEntryId;
            sealCallback = pending.isEmpty() ? this.sealed : null;
        }

        // Complete the futures and release buffer in a different thread.
        executor.execute(() -> {
            for (PendingChunkReplicaEntry toAck : pendingEntries) {
                toAck.completion.complete(null);
            }

            if (sealCallback != null) {
                sealCallback.complete(null);
                close();
            }
        });
    }

    private void sealComplete() {
        CompletableFuture<Void> sealCallback;
        synchronized (this.lock) {
            sealCallback = this.pending.isEmpty() ? this.sealed : null;
        }
        if (sealCallback != null) {
            sealCallback.complete(null);
        }
    }

    //region ResponseProcessor

    private final class ResponseProcessor extends ReplyProcessorBase {
        ResponseProcessor(String traceLogId, Consumer<Throwable> errorCallback) {
            super(traceLogId, errorCallback);
        }

        @Override
        public void chunkCreated(ChunkCreated chunkCreated) {
            log.info("{}: Log Chunk Replica created.", traceLogId);
            initialized.complete(null);
        }

        @Override
        public void entryAppended(EntryAppended entryAppended) {
            log.trace("{}: Ack Entry Id {}.", traceLogId, entryAppended.getUpToEntryId());
            try {
                ackUpTo(entryAppended.getUpToEntryId());
            } catch (Exception e) {
                failConnection(e);
            }
        }

        @Override
        public void chunkSealed(ChunkSealed reply) {
            log.info("{}: Log Chunk Replica Sealed.", traceLogId);
            sealComplete();
        }
    }

    //endregion

    @RequiredArgsConstructor
    private static class PendingChunkReplicaEntry {
        final Entry entry;
        final CompletableFuture<Void> completion = new CompletableFuture<>();
    }
}
