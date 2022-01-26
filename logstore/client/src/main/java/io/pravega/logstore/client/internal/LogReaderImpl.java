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

import io.pravega.logstore.client.Entry;
import io.pravega.logstore.client.LogClientConfig;
import io.pravega.logstore.client.LogInfo;
import io.pravega.logstore.client.LogReader;
import io.pravega.logstore.client.internal.connections.ClientConnectionFactory;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
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
        log.info("Initialized logReaderImpl for log {} with chunks",this.metadata.getLogId());
        this.metadata.getChunks().forEach( m -> log.info( "{},", m.getChunkId()));
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
        log.info("abhin LogReaderImpl: getNext called for log {}",this.getLogId());
        if (this.closed) {
            // We're done.
            log.info("LogReaderImpl: log closed {}",this.getLogId());
            return CompletableFuture.completedFuture(null);
        }

        if (this.currentReader == null) {
            log.info("inside logreaderimpl: current reader is null");
            // Advance reader and try again.
//            return advanceReader()
//                    .thenCompose(v -> getNext());

            CompletableFuture<Void> cf = advanceReader();
            CompletableFuture<List<Entry>> cfe = new CompletableFuture<>();
            cf.whenComplete( (r, ex) -> getNext().whenComplete( ( ret_list, thr) -> cfe.complete(ret_list) ));
            try {
                log.info("returning cfe...list entry size {}", cfe.get().size());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            return cfe;
        } else {
            log.info("curent reader is not null...getNext from current Reader");
            return this.currentReader.getNext()
                    .thenCompose(result -> {
                        if (result == null) {
                            return advanceReader().thenCompose(v -> getNext());
                        } else {
                            return CompletableFuture.completedFuture(result);
                        }
                    });
        }
    }

    private CompletableFuture<Void> advanceReader() {
        log.info("advance reader for log {}. Closing current reader",this.getLogId());
        closeCurrentReader();
        if (this.chunkIterator.hasNext()) {
            val cm = this.chunkIterator.next();
            val storeURI = cm.getLocations().stream().findFirst().orElse(null);
            assert storeURI != null;
            log.info("chunk iterator has next chunk {} on log {} for storeURI {}",cm.getChunkId(),getLogId(),storeURI);
            this.currentReader = new LogChunkReplicaReaderImpl(getLogId(), cm.getChunkId(), storeURI, connectionFactory.getInternalExecutor());
            return this.currentReader.initialize(connectionFactory);
        } else {
            // No more readers to read from. Close.
            log.info("no more readers to read from for log {}. Closing...",this.getLogId());
            close();
            return CompletableFuture.completedFuture(null);
        }
    }

    private void closeCurrentReader() {
        log.info("closing current REader. Inside closeCurrentReader ");
        val r = this.currentReader;
        if (r != null) {
            r.close();
            this.currentReader = null;
        }
    }
}
