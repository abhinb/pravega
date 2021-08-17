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

import io.netty.buffer.ByteBuf;
import io.pravega.common.concurrent.Futures;
import io.pravega.logstore.client.internal.connections.ClientConnectionFactory;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class LogChunkWriterImpl implements LogChunkWriter {
    @Getter
    private final long chunkId;
    private final String traceLogId;
    private final List<LogChunkWriter> replicaWriters;
    private final AtomicLong length;

    public LogChunkWriterImpl(long logId, long chunkId, int replicaCount, @NonNull LogServerManager logServerManager,
                              @NonNull Executor executor) {
        this.chunkId = chunkId;
        this.traceLogId = String.format("ChunkWriter[%s-%s]", logId, chunkId);
        val serverURIs = logServerManager.getServerUris(replicaCount);
        this.replicaWriters = serverURIs.stream()
                .map(uri -> new LogChunkReplicaWriterImpl(logId, chunkId, uri, executor))
                .collect(Collectors.toList());
        this.length = new AtomicLong(0L);
    }

    @Override
    public long getLastAckedEntryId() {
        return this.replicaWriters.stream().mapToLong(LogChunkWriter::getLastAckedEntryId).min().getAsLong();
    }

    @Override
    public long getLength() {
        return this.length.get();
    }

    @Override
    public boolean isSealed() {
        return this.replicaWriters.stream().anyMatch(LogChunkWriter::isSealed);
    }

    @Override
    public Collection<URI> getReplicaURIs() {
        return this.replicaWriters.stream()
                .map(LogChunkWriter::getReplicaURIs)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public CompletableFuture<Void> initialize(ClientConnectionFactory connectionFactory) {
        val futures = this.replicaWriters.stream()
                .map(w -> w.initialize(connectionFactory))
                .collect(Collectors.toList());
        return Futures.allOf(futures)
                .whenComplete((r, ex) -> {
                    if (ex != null) {
                        log.error("{}: Initialization failure. Closing.", this.traceLogId, ex);
                        close();
                        throw new CompletionException(ex);
                    } else {
                        log.info("{}: All replica writers initialized.", this.traceLogId);
                    }
                });
    }

    @Override
    public CompletableFuture<Void> addEntry(Entry entry) {
        this.length.addAndGet(entry.getLength());
        val futures = new ArrayList<CompletableFuture<Void>>(this.replicaWriters.size());
        for (val w : this.replicaWriters) {
            futures.add(w.addEntry(new PendingReplicaEntry(entry)));
        }

        val result = Futures.allOf(futures);
        result.exceptionally(ex -> {
            log.error("{}: addEntry failure. Closing.", this.traceLogId, ex);
            close();
            return null;
        });
        return result;
    }

    @Override
    public void close() {
        this.replicaWriters.forEach(LogChunkWriter::close);
        log.info("{}: Closed.", this.traceLogId);
    }

    @Getter
    private static class PendingReplicaEntry implements Entry {
        private final EntryAddress address;
        private final int crc32;
        private final int length;
        private final ByteBuf data;

        PendingReplicaEntry(Entry base) {
            this.address = base.getAddress();
            this.crc32 = base.getCrc32();
            this.length = base.getLength();
            this.data = base.getData().slice();
            assert this.length == this.data.readableBytes();
        }
    }
}
