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
import io.pravega.logstore.client.internal.connections.ConnectionFactory;
import io.pravega.logstore.client.internal.connections.ConnectionFailedException;
import io.pravega.logstore.shared.LogChunkExistsException;
import io.pravega.logstore.shared.LogChunkNotExistsException;
import io.pravega.logstore.shared.protocol.ReplyProcessor;
import io.pravega.logstore.shared.protocol.commands.AppendEntry;
import io.pravega.logstore.shared.protocol.commands.ChunkAlreadyExists;
import io.pravega.logstore.shared.protocol.commands.ChunkCreated;
import io.pravega.logstore.shared.protocol.commands.ChunkNotExists;
import io.pravega.logstore.shared.protocol.commands.EntryAppended;
import io.pravega.logstore.shared.protocol.commands.ErrorMessage;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class LogChunkReplicaWriterImpl implements LogChunkReplicaWriter {
    @Getter
    private final long chunkId;
    private final URI logStoreUri;
    private final CompletableFuture<ClientConnection> connection;
    private final Executor executor;
    private final State state;
    private final ResponseProcessor responseProcessor;
    private final Object writeOrderLock = new Object();
    private final String traceLogId;

    public LogChunkReplicaWriterImpl(long chunkId, @NonNull URI logStoreUri, @NonNull ConnectionFactory connectionFactory, @NonNull Executor executor) {
        this.chunkId = chunkId;
        this.logStoreUri = logStoreUri;
        this.executor = executor;
        this.responseProcessor = new ResponseProcessor();
        this.connection = connectionFactory.establishConnection(logStoreUri, this.responseProcessor);
        this.state = new State();
        this.traceLogId = String.format("[%s-%s]", chunkId, logStoreUri);
    }

    @Override
    public long getLastAckedEntryId() {
        return this.state.getLastAckedEntryId();
    }

    @Override
    public void addEntry(PendingAddEntry entry) {
        Exceptions.checkNotClosed(this.state.isClosed(), this);
        Preconditions.checkArgument(entry.getChunkId() == this.chunkId);

        synchronized (writeOrderLock) {
            try {
                val connection = Futures.getThrowingException(this.connection);
                val ae = new AppendEntry(entry.getChunkId(), entry.getEntryId(), entry.getCrc32(), entry.getData());
                log.trace("{}: Sending AppendEntry: {}", this, ae);
                connection.send(ae);
                state.addPending(entry);
            } catch (Throwable ex) {
                entry.getCompletion().completeExceptionally(ex);
                log.error("{}: Failed to add new entry {}. ", this.traceLogId, entry, ex);
            }
        }
    }

    @Override
    public void close() {
        state.close();
        this.connection.join().close();
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
        private final ArrayDeque<PendingAddEntry> pending = new ArrayDeque<>();
        @GuardedBy("lock")
        private long nextExpectedEntryId = 0;
        @GuardedBy("lock")
        private long lastAckedEntryId;

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
            log.info("{}: Handling exception.", traceLogId, ex);
            this.closed = true;
            val pending = removeAllPending();
            executor.execute(() -> {
                for (PendingAddEntry toAck : pending) {
                    try {
                        toAck.getCompletion().complete(null);
                    } finally {
                        toAck.getData().release();
                    }
                }
            });
        }

        void ackUpTo(long lastAckedEntryId) {
            final List<PendingAddEntry> pendingEntries = removePending(lastAckedEntryId);
            // Complete the futures and release buffer in a different thread.
            executor.execute(() -> {
                for (PendingAddEntry toAck : pendingEntries) {
                    try {
                        toAck.getCompletion().complete(null);
                    } finally {
                        toAck.getData().release();
                    }
                }
            });
        }

        /**
         * Add event to the infight
         *
         * @return The EventNumber for the event.
         */
        private void addPending(PendingAddEntry entry) {
            synchronized (lock) {
                Preconditions.checkArgument(entry.getEntryId() == this.nextExpectedEntryId,
                        "Unexpected EntryId. Expected %s, given %s.", this.nextExpectedEntryId, entry.getEntryId());
                log.trace("{}: Adding {} to inflight.", traceLogId, entry);
                pending.addLast(entry);
                this.nextExpectedEntryId++;
            }
        }

        /**
         * Remove all events with event numbers below the provided level from inflight and return them.
         */
        private List<PendingAddEntry> removePending(long upToEntryId) {
            synchronized (lock) {
                List<PendingAddEntry> result = new ArrayList<>();
                PendingAddEntry entry = pending.peekFirst();
                while (entry != null && entry.getEntryId() <= upToEntryId) {
                    pending.pollFirst();
                    result.add(entry);
                    entry = pending.peekFirst();
                }

                this.lastAckedEntryId = upToEntryId;
                return result;
            }
        }

        private List<PendingAddEntry> removeAllPending() {
            synchronized (lock) {
                List<PendingAddEntry> inflightEvents = new ArrayList<>(pending);
                pending.clear();
                return inflightEvents;
            }
        }

        private Long getLowestPendingEntryId() {
            synchronized (lock) {
                PendingAddEntry entry = pending.peekFirst();
                return entry == null ? null : entry.getEntryId();
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
        public void chunkCreated(ChunkCreated chunkCreated) {
            log.info("{}: Log Chunk Replica created.", traceLogId);
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
}
