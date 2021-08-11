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
import io.pravega.logstore.server.LogStoreConfig;
import io.pravega.logstore.server.chunks.ChunkReplicaManager;
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

    public CompletableFuture<Void> createChunk(long chunkId) {
        log.info("Create Chunk {}.", chunkId);
        return this.manager.createChunkReplica(chunkId);
    }

    public CompletableFuture<Long> appendEntry(@NonNull ChunkEntry entry) {
        log.debug("Append {}.", entry);
        val writer = this.manager.getWriter(entry.getChunkId());
        return writer.append(entry);
    }

}
