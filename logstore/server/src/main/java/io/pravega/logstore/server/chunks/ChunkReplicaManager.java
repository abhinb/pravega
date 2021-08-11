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

import io.pravega.common.Exceptions;
import io.pravega.logstore.shared.LogChunkExistsException;
import io.pravega.logstore.shared.LogChunkNotExistsException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@RequiredArgsConstructor
public class ChunkReplicaManager implements AutoCloseable {
    @NonNull
    private final ChunkReplicaFactory chunkReplicaFactory;
    @NonNull
    private final ScheduledExecutorService writeExecutor;
    @NonNull
    private final ScheduledExecutorService readExecutor;
    private final ConcurrentHashMap<Long, ChunkReplicaWriter> chunkWriters = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            val writers = new ArrayList<>(this.chunkWriters.values());
            this.chunkWriters.clear();
            writers.forEach(ChunkReplicaWriter::close);
            log.info("Closed.");
        }
    }

    public CompletableFuture<Void> createChunkReplica(long chunkId) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        log.info("Create Chunk {}.", chunkId);
        val newWriter = new AtomicReference<ChunkReplicaWriter>();
        return CompletableFuture
                .runAsync(() -> {
                    newWriter.set(this.chunkReplicaFactory.createChunkReplica(chunkId));
                    newWriter.get().setOnClose(() -> unregisterWriter(chunkId, newWriter.get()));

                    val existingWriter = this.chunkWriters.putIfAbsent(chunkId, newWriter.get());
                    if (existingWriter != null) {
                        // We already have this created.
                        throw new LogChunkExistsException(chunkId);
                    }

                    try {
                        newWriter.get().start();
                    } catch (FileAlreadyExistsException ex) {
                        throw new LogChunkExistsException(chunkId);
                    } catch (Throwable ex) {
                        throw new CompletionException(ex);
                    }

                    // Check again, in case we've already registered this.
                    Exceptions.checkNotClosed(this.closed.get(), this);
                }, this.writeExecutor)
                .whenComplete((r, ex) -> {
                    if (ex != null) {
                        val w = newWriter.get();
                        if (w != null) {
                            // Close this writer.
                            try {
                                w.close();
                            } catch (Throwable ex2) {
                                log.error("Unable to close newly created writer that errored out.", ex2);
                            }
                        }
                        throw new CompletionException(Exceptions.unwrap(ex));
                    }
                });
    }

    public ChunkReplicaWriter getWriter(long chunkId) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        val writer = this.chunkWriters.getOrDefault(chunkId, null);
        if (writer == null) {
            throw new LogChunkNotExistsException(chunkId);
        }
        return writer;
    }

    private void unregisterWriter(long chunkId, ChunkReplicaWriter writer) {
        if (this.closed.get()) {
            return;
        }

        if (!this.chunkWriters.remove(chunkId, writer)) {
            log.debug("Attempted to unregister non-registered writer for ChunkId {}.", chunkId);
        }
    }
}
