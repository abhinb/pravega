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

import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@RequiredArgsConstructor
@Slf4j
public class LogStoreService {
    private final LogStoreConfig logStoreConfig;
    private final ScheduledExecutorService coreExecutor;
    private final ScheduledExecutorService writeExecutor;
    private final ScheduledExecutorService readExecutor;

    public CompletableFuture<Void> createChunk(long chunkId) {
        log.info("Create Chunk {}.", chunkId);
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> appendEntry(long chunkId, long entryId, ByteBuf data, int crc32) {
        log.info("Append Entry ChunkId={}, EntryId={}, Crc={}, Length={}.", chunkId, entryId, data.readableBytes(), crc32); // TODO debug
        return CompletableFuture.completedFuture(null);
    }
}
