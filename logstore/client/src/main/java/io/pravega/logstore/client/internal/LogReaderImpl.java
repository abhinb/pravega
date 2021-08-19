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

import io.pravega.logstore.client.LogClientConfig;
import io.pravega.logstore.client.LogInfo;
import io.pravega.logstore.client.LogReader;
import io.pravega.logstore.client.internal.connections.ClientConnectionFactory;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
import lombok.val;

public class LogReaderImpl implements LogReader {
    private final LogClientConfig config;
    private final ClientConnectionFactory connectionFactory;
    private final String traceLogId;
    private final LogMetadata metadata;
    private final Iterator<LogMetadata.ChunkMetadata> chunkIterator;
    private volatile LogChunkReader currentReader = null;
    private volatile boolean closed = false;

    public LogReaderImpl(@NonNull LogMetadata logMetadata, @NonNull LogClientConfig config, @NonNull ClientConnectionFactory connectionFactory) {
        this.metadata = logMetadata;
        this.config = config;
        this.connectionFactory = connectionFactory;
        this.chunkIterator = this.metadata.getChunks().stream().limit(this.metadata.getChunks().size() - 1).iterator();
        this.traceLogId = String.format("LogReader[%s]", getLogId());
    }

    @Override
    public long getLogId() {
        return this.metadata.getLogId();
    }

    @Override
    public LogInfo getInfo() {
        return new LogInfo(getLogId(), this.metadata.getChunks().size(), this.metadata.getEpoch());
    }

    @Override
    public void close() {
        if (!this.closed) {
            closeCurrentReader();
            this.closed = true;
        }
    }

    @Override
    public CompletableFuture<List<Entry>> getNext() {
        // Iterate through Chunks.
        // Return iterators of chunks.
        if (this.closed) {
            // We're done.
            return CompletableFuture.completedFuture(null);
        }

        if (this.currentReader == null) {
            // Advance reader and try again.
            return advanceReader()
                    .thenComposeAsync(v -> getNext());
        } else {
            return this.currentReader.getNext()
                    .thenCompose(result -> {
                        if (result == null) {
                            return advanceReader().thenComposeAsync(v -> getNext());
                        } else {
                            return CompletableFuture.completedFuture(result);
                        }
                    });
        }
    }

    private CompletableFuture<Void> advanceReader() {
        closeCurrentReader();
        if (this.chunkIterator.hasNext()) {
            val cm = this.chunkIterator.next();
            val storeURI = cm.getLocations().stream().findFirst().orElse(null);
            assert storeURI != null;
            this.currentReader = new LogChunkReplicaReaderImpl(getLogId(), cm.getChunkId(), storeURI, connectionFactory.getInternalExecutor());
            return this.currentReader.initialize(connectionFactory);
        } else {
            // No more readers to read from. Close.
            close();
            return CompletableFuture.completedFuture(null);
        }
    }

    private void closeCurrentReader() {
        val r = this.currentReader;
        if (r != null) {
            r.close();
            this.currentReader = null;
        }
    }
}
