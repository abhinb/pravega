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
package io.pravega.logstore.server.service;

import io.pravega.logstore.server.ChunkEntry;
import io.pravega.logstore.server.ChunkInfo;
import io.pravega.logstore.server.LogStoreConfig;
import io.pravega.logstore.server.chunks.ChunkReplicaManager;
import io.pravega.logstore.server.chunks.ChunkReplicaReader;
import io.pravega.logstore.shared.LogChunkNotExistsException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;


@RequiredArgsConstructor
@Slf4j
public class LogStoreService {
    @NonNull
    private final ChunkReplicaManager manager;
    @NonNull
    private final LogStoreConfig logStoreConfig;
    @NonNull
    private final ScheduledExecutorService coreExecutor;
    @NonNull
    private final ScheduledExecutorService writeExecutor;
    @NonNull
    private final ScheduledExecutorService readExecutor;

    public CompletableFuture<Void> createChunk(long chunkId) {
        log.info("Create Chunk {}.", chunkId);
        return this.manager.createChunkReplica(chunkId);
    }

    public CompletableFuture<Void> sealChunks(Collection<Long> chunkIds) {
        log.info("Seal Chunks {}", chunkIds);
        return CompletableFuture.runAsync(() -> {
            for (val chunkId : chunkIds) {
                try {
                    log.info("Seal Chunk {}", chunkId);
                    val writer = this.manager.getWriter(chunkId);
                    writer.close();
                } catch (LogChunkNotExistsException ex) {
                    log.debug("Not sealing chunk {} because no writer could be found for it.", chunkId);
                    continue;
                }
            }
        }, this.writeExecutor);
    }

    public CompletableFuture<Long> appendEntry(@NonNull ChunkEntry entry) {
        log.debug("Append {}.", entry);
        val writer = this.manager.getWriter(entry.getChunkId());
        return writer.append(entry);
    }

    public CompletableFuture<ChunkInfo> getChunkInfo(long chunkId) {
        return this.manager.getReader(chunkId)
                .thenComposeAsync(ChunkReplicaReader::getInfo, this.readExecutor);
    }

    public CompletableFuture<List<ChunkEntry>> readEntries(long chunkId, long entryId) {
        return readEntries(chunkId, entryId, this.logStoreConfig.getMaxReadSize());
    }

    public CompletableFuture<List<ChunkEntry>> readEntries(long chunkId, long entryId, int maxSizeBytes) {
        log.debug("Read ChunkId={}, EntryId={}, MaxSizeBytes={}", chunkId, entryId, maxSizeBytes);
        return this.manager.getReader(chunkId)
                .thenComposeAsync(reader -> reader.read(entryId, maxSizeBytes), this.readExecutor);
    }

}
