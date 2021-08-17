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
import io.pravega.logstore.client.internal.connections.ClientConnection;
import io.pravega.logstore.client.internal.connections.ClientConnectionFactory;
import io.pravega.logstore.client.internal.connections.ConnectionFailedException;
import io.pravega.logstore.shared.LogChunkExistsException;
import io.pravega.logstore.shared.LogChunkNotExistsException;
import io.pravega.logstore.shared.protocol.ReplyProcessor;
import io.pravega.logstore.shared.protocol.commands.AbstractCommand;
import io.pravega.logstore.shared.protocol.commands.AppendEntry;
import io.pravega.logstore.shared.protocol.commands.ChunkAlreadyExists;
import io.pravega.logstore.shared.protocol.commands.ChunkCreated;
import io.pravega.logstore.shared.protocol.commands.ChunkNotExists;
import io.pravega.logstore.shared.protocol.commands.CreateChunk;
import io.pravega.logstore.shared.protocol.commands.EntryAppended;
import io.pravega.logstore.shared.protocol.commands.ErrorMessage;
import io.pravega.logstore.shared.protocol.commands.Hello;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class LogChunkReplicaWriterImpl implements LogChunkWriter {
    @Getter
    private final long chunkId;
    private final URI logStoreUri;
    private volatile ClientConnection connection;
    private final Executor executor;
    private final State state;
    private final Object writeOrderLock = new Object();
    private final String traceLogId;

    public LogChunkReplicaWriterImpl(long logId, long chunkId, @NonNull URI logStoreUri,
                                     @NonNull Executor executor) {
        this.chunkId = chunkId;
        this.logStoreUri = logStoreUri;
        this.executor = executor;
        this.state = new State();
        this.traceLogId = String.format("ChunkWriter[%s-%s-%s:%s]", logId, chunkId, logStoreUri.getHost(), logStoreUri.getPort());
    }

    @Override
    public CompletableFuture<Void> initialize(@NonNull ClientConnectionFactory connectionFactory) {
        val responseProcessor = new ResponseProcessor();
        try {
            connectionFactory.establishConnection(logStoreUri, responseProcessor)
                    .thenAccept(connection -> {
                        this.connection = connection;
                        createLogChunk();
                    })
                    .whenComplete((r, ex) -> {
                        if (ex != null) {
                            state.fail(ex);
                            close();
                        }
                    });
            return state.initialized;
        } catch (Throwable ex) {
            state.fail(ex);
            close();
            return Futures.failedFuture(ex);
        }
    }

    @Override
    public long getLastAckedEntryId() {
        return this.state.getLastAckedEntryId();
    }

    @Override
    public long getLength() {
        return 0;
    }

    @Override
    public boolean isSealed() {
        return this.state.isClosed();
    }

    @Override
    public Collection<URI> getReplicaURIs() {
        return Collections.singleton(this.logStoreUri);
    }

    @SneakyThrows
    private void createLogChunk() {
        val cc = new CreateChunk(0L, this.chunkId);
        connection.send(cc);
    }

    @Override
    public CompletableFuture<Void> addEntry(Entry entry) {
        Exceptions.checkNotClosed(this.state.isClosed(), this);
        Preconditions.checkState(this.state.isInitialized(), "Not initialized.");
        Preconditions.checkArgument(entry.getAddress().getChunkId() == this.chunkId);

        synchronized (writeOrderLock) {
            try {
                val connection = this.connection;
                val ae = new AppendEntry(entry.getAddress().getChunkId(), entry.getAddress().getEntryId(), entry.getCrc32(), entry.getData());
                log.trace("{}: Sending AppendEntry: {}", this, ae);
                connection.send(ae);
                return state.addPending(entry);
            } catch (Throwable ex) {
                log.error("{}: Failed to add new entry {}. ", this.traceLogId, entry, ex);
                return Futures.failedFuture(ex);
            }
        }
    }

    @Override
    public void close() {
        state.close();
        val connection = this.connection;
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public String toString() {
        return this.traceLogId;
    }

    //region State

    @ToString(of = {"closed", "nextExpectedEntryId", "lastAckedEntryId"})
    @RequiredArgsConstructor
    private final class State implements AutoCloseable {
        private final Object lock = new Object();
        @Getter
        private volatile boolean closed = false;
        @GuardedBy("lock")
        private final ArrayDeque<PendingChunkReplicaEntry> pending = new ArrayDeque<>();
        @GuardedBy("lock")
        private long nextExpectedEntryId = 0;
        @GuardedBy("lock")
        private long lastAckedEntryId;
        private final AtomicLong length = new AtomicLong(0);
        private final CompletableFuture<Void> initialized = new CompletableFuture<>();

        boolean isInitialized() {
            return this.initialized.isDone();
        }

        @Override
        public void close() {
            fail(new CancellationException("Closing."));
            this.closed = true;
        }

        private long getNextExpectedEntryId() {
            synchronized (lock) {
                return this.nextExpectedEntryId;
            }
        }

        private long getLastAckedEntryId() {
            synchronized (lock) {
                return this.lastAckedEntryId;
            }
        }

        /**
         * @param ex Error that has occurred that needs to be handled by tearing down the connection.
         */
        void fail(Throwable ex) {
            log.info("{}: Handling exception {}.", traceLogId, ex.toString());
            this.closed = true;
            val pending = removeAllPending();
            executor.execute(() -> {
                this.initialized.completeExceptionally(ex);
                for (PendingChunkReplicaEntry toAck : pending) {
                    toAck.completion.complete(null);
                }
            });
        }

        void ackUpTo(long lastAckedEntryId) {
            final List<PendingChunkReplicaEntry> pendingEntries = removePending(lastAckedEntryId);
            // Complete the futures and release buffer in a different thread.
            executor.execute(() -> {
                for (PendingChunkReplicaEntry toAck : pendingEntries) {
                    this.length.addAndGet(toAck.entry.getLength());
                    toAck.completion.complete(null);
                }
            });
        }

        /**
         * Add event to the infight
         *
         * @return The EventNumber for the event.
         */
        private CompletableFuture<Void> addPending(Entry entry) {
            val pe = new PendingChunkReplicaEntry(entry);
            synchronized (lock) {
                Preconditions.checkArgument(entry.getAddress().getEntryId() == this.nextExpectedEntryId,
                        "Unexpected EntryId. Expected %s, given %s.", this.nextExpectedEntryId, entry.getAddress().getEntryId());
                log.trace("{}: Adding {} to inflight.", traceLogId, entry);
                pending.addLast(pe);
                this.nextExpectedEntryId++;
            }
            return pe.completion;
        }

        /**
         * Remove all events with event numbers below the provided level from inflight and return them.
         */
        private List<PendingChunkReplicaEntry> removePending(long upToEntryId) {
            synchronized (lock) {
                List<PendingChunkReplicaEntry> result = new ArrayList<>();
                PendingChunkReplicaEntry entry = pending.peekFirst();
                while (entry != null && entry.entry.getAddress().getEntryId() <= upToEntryId) {
                    pending.pollFirst();
                    result.add(entry);
                    entry = pending.peekFirst();
                }

                this.lastAckedEntryId = upToEntryId;
                return result;
            }
        }

        private List<PendingChunkReplicaEntry> removeAllPending() {
            synchronized (lock) {
                List<PendingChunkReplicaEntry> pending = new ArrayList<>(this.pending);
                this.pending.clear();
                return pending;
            }
        }

        private Long getLowestPendingEntryId() {
            synchronized (lock) {
                PendingChunkReplicaEntry entry = pending.peekFirst();
                return entry == null ? null : entry.entry.getAddress().getEntryId();
            }
        }
    }

    //endregion

    //region ResponseProcessor

    private final class ResponseProcessor implements ReplyProcessor {
        @Override
        public void connectionDropped() {
            failConnection(new ConnectionFailedException(String.format("Connection dropped for %s.", traceLogId)));
        }

        @Override
        public void hello(Hello hello) {
            if (hello.getLowVersion() > AbstractCommand.WIRE_VERSION || hello.getHighVersion() < AbstractCommand.OLDEST_COMPATIBLE_VERSION) {
                log.error("Incompatible wire protocol versions {}", hello);
            } else {
                log.info("Received hello: {}", hello);
            }
        }

        @Override
        public void chunkAlreadyExists(ChunkAlreadyExists alreadyExists) {
            log.info("{}: Log Chunk Replica already exists.", traceLogId);
            state.fail(new LogChunkExistsException(alreadyExists.getChunkId()));
            close();
        }

        @Override
        public void chunkNotExists(ChunkNotExists notExists) {
            log.info("{}: Log Chunk Replica does not exist.", traceLogId);
            state.fail(new LogChunkNotExistsException(notExists.getChunkId()));
            close();
        }

        @Override
        public void chunkCreated(ChunkCreated chunkCreated) {
            log.info("{}: Log Chunk Replica created.", traceLogId);
            state.initialized.complete(null);
        }

        @Override
        public void error(ErrorMessage errorMessage) {
            log.info("{}: General error {}: {}.", traceLogId, errorMessage.getErrorCode(), errorMessage.getMessage());
            state.fail(errorMessage.getThrowableException());
        }

        @Override
        public void entryAppended(EntryAppended entryAppended) {
            log.trace("{}: Ack Entry Id {}.", traceLogId, entryAppended.getUpToEntryId());
            try {
                state.ackUpTo(entryAppended.getUpToEntryId());
            } catch (Exception e) {
                failConnection(e);
            }
        }

        @Override
        public void processingFailure(Exception error) {
            failConnection(error);
        }

        private void failConnection(Throwable e) {
            log.warn("{}: Failing connection with exception {}", traceLogId, e.toString());
            state.fail(Exceptions.unwrap(e));
        }
    }

    //endregion

    @RequiredArgsConstructor
    private static class PendingChunkReplicaEntry {
        final Entry entry;
        final CompletableFuture<Void> completion = new CompletableFuture<>();
    }
}
