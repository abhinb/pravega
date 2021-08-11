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

import com.google.common.base.Preconditions;
import io.pravega.logstore.server.LogStoreConfig;
import java.nio.file.Path;
import java.util.concurrent.ScheduledExecutorService;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@RequiredArgsConstructor
public class ChunkReplicaFactory {
    @NonNull
    private final LogStoreConfig config;
    @NonNull
    private final ScheduledExecutorService writeExecutor;
    private volatile boolean initialized;

    public void initialize() {
        if (this.initialized) {
            return;
        }

        // Create path(s).
        val storagePath = Path.of(this.config.getStoragePath());
        val f = storagePath.toFile();
        if (f.exists()) {
            Preconditions.checkArgument(f.isDirectory(), "Storage Path '%s' is not a directory.", storagePath);
        } else {
            boolean created = f.mkdirs();
            Preconditions.checkState(created, "Unable to create Storage Path '%s'.", storagePath);
        }

        this.initialized = true;
        log.info("Initialized.");
    }

    public ChunkReplicaWriter createChunkReplica(long chunkId) {
        Preconditions.checkState(this.initialized, "Not initialized.");
        return new ChunkReplicaWriter(chunkId, this.config, this.writeExecutor);
    }
}
