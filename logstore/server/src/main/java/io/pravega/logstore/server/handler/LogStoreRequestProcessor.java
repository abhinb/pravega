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
package io.pravega.logstore.server.handler;

import io.pravega.common.Exceptions;
import io.pravega.common.tracing.TagLogger;
import io.pravega.logstore.server.ChunkEntry;
import io.pravega.logstore.server.service.LogStoreService;
import io.pravega.logstore.shared.BadEntryIdException;
import io.pravega.logstore.shared.LogChunkExistsException;
import io.pravega.logstore.shared.LogChunkNotExistsException;
import io.pravega.logstore.shared.protocol.RequestProcessor;
import io.pravega.logstore.shared.protocol.commands.AbstractCommand;
import io.pravega.logstore.shared.protocol.commands.AppendEntry;
import io.pravega.logstore.shared.protocol.commands.BadEntryId;
import io.pravega.logstore.shared.protocol.commands.ChunkAlreadyExists;
import io.pravega.logstore.shared.protocol.commands.ChunkCreated;
import io.pravega.logstore.shared.protocol.commands.ChunkInfo;
import io.pravega.logstore.shared.protocol.commands.ChunkNotExists;
import io.pravega.logstore.shared.protocol.commands.ChunkSealed;
import io.pravega.logstore.shared.protocol.commands.CreateChunk;
import io.pravega.logstore.shared.protocol.commands.EntriesRead;
import io.pravega.logstore.shared.protocol.commands.EntryAppended;
import io.pravega.logstore.shared.protocol.commands.EntryData;
import io.pravega.logstore.shared.protocol.commands.ErrorMessage;
import io.pravega.logstore.shared.protocol.commands.GetChunkInfo;
import io.pravega.logstore.shared.protocol.commands.Hello;
import io.pravega.logstore.shared.protocol.commands.KeepAlive;
import io.pravega.logstore.shared.protocol.commands.ReadEntries;
import io.pravega.logstore.shared.protocol.commands.SealChunk;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.slf4j.LoggerFactory;

import static io.pravega.common.function.Callbacks.invokeSafely;

@RequiredArgsConstructor
public class LogStoreRequestProcessor implements RequestProcessor {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(LogStoreRequestProcessor.class));
    @NonNull
    private final LogStoreService service;
    @NonNull
    private final TrackedConnection connection;
    private final Set<Long> activeChunkIds = Collections.synchronizedSet(new HashSet<>());
    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.connection.close();
            log.debug("{} Closed connection.", this.connection);
            this.service.sealChunks(this.activeChunkIds)
                    .thenRun(() -> {
                        log.info("{} Closed and sealed {} chunk(s).", this.connection, this.activeChunkIds.size());
                        this.activeChunkIds.clear();
                    });
        }
    }

    @Override
    public void hello(@NonNull Hello hello) {
        log.info("Received {} from connection {}.", hello, this.connection);
        this.connection.send(new Hello(AbstractCommand.WIRE_VERSION, AbstractCommand.OLDEST_COMPATIBLE_VERSION));
        if (hello.getLowVersion() > AbstractCommand.WIRE_VERSION || hello.getHighVersion() < AbstractCommand.OLDEST_COMPATIBLE_VERSION) {
            log.warn(hello.getRequestId(), "Incompatible wire protocol versions {} from connection {}", hello, connection);
            close();
        }
    }

    @Override
    public void keepAlive(KeepAlive keepAlive) {
        log.trace("{} KeepAlive.", this.connection);
    }

    @Override
    public void createChunk(@NonNull CreateChunk request) {
        log.info(request.getRequestId(), "{}: Creating Log Chunk {}.", this.connection, request);
        this.service.createChunk(request.getChunkId())
                .thenRun(() -> {
                    this.activeChunkIds.add(request.getChunkId());
                    this.connection.send(new ChunkCreated(request.getRequestId(), request.getChunkId()));
                })
                .whenComplete((r, ex) -> {
                    if (ex == null) {
                        log.debug(request.getRequestId(), "{}: Created Log Chunk {}.", this.connection, request.getChunkId());
                    } else {
                        handleException(request.getRequestId(), request.getChunkId(), "createChunk", ex);
                    }
                });
    }

    @Override
    public void sealChunk(@NonNull SealChunk request) {
        log.info(request.getRequestId(), "{}: Sealing Log Chunk {}.", this.connection, request);
        this.service.sealChunks(Collections.singleton(request.getChunkId()))
                .thenRun(() -> {
                    this.activeChunkIds.remove(request.getChunkId());
                    this.connection.send(new ChunkSealed(request.getRequestId(), request.getChunkId()));
                })
                .whenComplete((r, ex) -> {
                    if (ex == null) {
                        log.debug(request.getRequestId(), "{}: Seal Log Chunk {}.", this.connection, request.getChunkId());
                    } else {
                        handleException(request.getRequestId(), request.getChunkId(), "sealChunk", ex);
                    }
                });
    }

    @Override
    public void appendEntry(@NonNull AppendEntry request) {
        val wireEntry = request.getEntry();
        log.debug(request.getRequestId(), "{}: Append ChunkId={}, EntryId={}, Crc={}, Length={}.",
                this.connection, request.getChunkId(), wireEntry.getEntryId(), wireEntry.getCrc32(), wireEntry.getData().readableBytes());
        val entry = new ChunkEntry(request.getChunkId(), wireEntry.getEntryId(), wireEntry.getCrc32(), wireEntry.getData());
        this.connection.adjustOutstandingBytes(entry.getLength());
        this.service.appendEntry(entry)
                .thenRun(() -> connection.send(new EntryAppended(request.getChunkId(), wireEntry.getEntryId()))) // TODO: serialize all this (reduce chatter).
                .whenComplete((r, ex) -> {
                    if (ex == null) {
                        log.debug("{}: Wrote Entry {} to Chunk {}.", this.connection, wireEntry.getEntryId(), request.getChunkId());
                    } else {
                        handleException(request.getRequestId(), request.getChunkId(), "appendEntry", ex);
                    }
                })
                .whenComplete((v, e) -> {
                    this.connection.adjustOutstandingBytes(-entry.getLength());
                    request.release(); // Release the buffers when done.
                });
    }

    @Override
    public void readEntries(ReadEntries request) {
        log.info(request.getRequestId(), "{}: Read Entries {}.", this.connection, request);
        this.service.readEntries(request.getChunkId(), request.getFromEntryId())
                .thenAccept(entries -> {
                    val wireEntries = entries.stream()
                            .map(e -> new EntryData(e.getEntryId(), e.getCrc32(), e.getData()))
                            .collect(Collectors.toList());
                    this.connection.send(new EntriesRead(request.getRequestId(), request.getChunkId(), wireEntries));
                })
                .whenComplete((r, ex) -> {
                    if (ex == null) {
                        log.debug(request.getRequestId(), "{}: Read Entries {} from EntryId {}.", this.connection, request.getChunkId(), request.getFromEntryId());
                    } else {
                        handleException(request.getRequestId(), request.getChunkId(), "readEntries", ex);
                    }
                });
    }

    @Override
    public void getChunkInfo(GetChunkInfo request) {
        log.info(request.getRequestId(), "{}: Get Chunk Info {}.", this.connection, request);
        this.service.getChunkInfo(request.getChunkId())
                .thenAccept(info -> this.connection.send(new ChunkInfo(request.getRequestId(), request.getChunkId(), info.getEntryCount(), info.getDataLength())))
                .whenComplete((r, ex) -> {
                    if (ex == null) {
                        log.debug(request.getRequestId(), "{}: Get Info {}.", this.connection, request.getChunkId());
                    } else {
                        handleException(request.getRequestId(), request.getChunkId(), "readEntries", ex);
                    }
                });
    }

    private void handleException(long requestId, long chunkId, String operation, Throwable u) {
        if (u == null) {
            IllegalStateException exception = new IllegalStateException("No exception to handle.");
            logError(requestId, chunkId, operation, u);
            throw exception;
        }

        u = Exceptions.unwrap(u);
        final Consumer<Throwable> failureHandler = t -> {
            logError(requestId, chunkId, operation, t);
            close();
        };

        if (u instanceof LogChunkExistsException) {
            log.info(requestId, "LogChunk '{}' already exists.", chunkId);
            invokeSafely(connection::send, new ChunkAlreadyExists(requestId, chunkId), failureHandler);
        } else if (u instanceof LogChunkNotExistsException) {
            log.warn(requestId, "LogChunk '{}' does not exist.", chunkId);
            invokeSafely(connection::send, new ChunkNotExists(requestId, chunkId), failureHandler);
        } else if (u instanceof BadEntryIdException) {
            BadEntryIdException badId = (BadEntryIdException) u;
            log.info(requestId, "Bad Entry Id for Log Chunk '{}'. Expected {}, given {}.",
                    badId.getChunkId(), badId.getExpectedEntryId(), badId.getProvidedEntryId());
            invokeSafely(connection::send, new BadEntryId(requestId, badId.getChunkId(), badId.getExpectedEntryId(), badId.getProvidedEntryId()), failureHandler);
        } else {
            logError(requestId, chunkId, operation, u);
            invokeSafely(connection::send, new ErrorMessage(requestId, chunkId, u.getClass().getSimpleName(), u.getMessage()), failureHandler);
            close(); // Closing connection should reinitialize things, and hopefully fix the problem
            throw new IllegalStateException("Unknown exception.", u);
        }
    }

    private void logError(long requestId, long chunkId, String operation, Throwable u) {
        if (u instanceof CancellationException) {
            log.warn(requestId, "Cancelled (LogChunkId = '{}', Operation = '{}')", chunkId, operation);
        } else {
            log.error(requestId, "Error (LogChunkId = '{}', Operation = '{}')", chunkId, operation, u);
        }
    }
}
