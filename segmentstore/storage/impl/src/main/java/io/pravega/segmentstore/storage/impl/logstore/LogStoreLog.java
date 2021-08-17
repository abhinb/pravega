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
package io.pravega.segmentstore.storage.impl.logstore;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.common.util.CloseableIterator;
import io.pravega.common.util.CompositeArrayView;
import io.pravega.logstore.client.LogWriter;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.segmentstore.storage.QueueStats;
import io.pravega.segmentstore.storage.ThrottleSourceListener;
import io.pravega.segmentstore.storage.WriteSettings;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;

public class LogStoreLog implements DurableDataLog {
    /**
     * Identical to BookKeeperLog.
     */
    @Getter
    private final WriteSettings writeSettings;

    private final LogWriter logWriter;

    LogStoreLog(@NonNull LogWriter logWriter) {
        this.logWriter = logWriter;
        val qs = this.logWriter.getQueueStatistics();
        this.writeSettings = new WriteSettings(qs.getMaxWriteLength(), Duration.ofSeconds(60), 256 * 1024 * 1024);
    }

    @Override
    public void close() {
        this.logWriter.close();
    }

    @Override
    public void initialize(Duration timeout) throws DurableDataLogException {
        this.logWriter.initialize().join(); // TODO not cool
    }

    @Override
    public CompletableFuture<LogAddress> append(CompositeArrayView data, Duration timeout) {
        val buffer = convertData(data);
        return this.logWriter.append(buffer)
                .thenApply(LogEntryAddress::new);
    }

    private ByteBuf convertData(CompositeArrayView data) {
        ByteBuf[] components = new ByteBuf[data.getComponentCount()];
        val index = new AtomicInteger();
        data.collect(bb -> components[index.getAndIncrement()] = Unpooled.wrappedBuffer(bb));
        return Unpooled.wrappedUnmodifiableBuffer(components);
    }

    @Override
    public CloseableIterator<ReadItem, DurableDataLogException> getReader() throws DurableDataLogException {
        return null; // TODO implement this, check QueueStatistics
    }

    @Override
    public long getEpoch() {
        return this.logWriter.getInfo().getEpoch();
    }

    @Override
    public QueueStats getQueueStatistics() {
        val rawStats = this.logWriter.getQueueStatistics();
        return new QueueStats(rawStats.getSize(), rawStats.getTotalLength(), rawStats.getMaxWriteLength(), rawStats.getExpectedProcessingTimeMillis());
    }

    @Override
    public void registerQueueStateChangeListener(ThrottleSourceListener listener) {
        // Not implemented.
    }

    @Override
    public void enable() throws DurableDataLogException {
        // Not implemented.
    }

    @Override
    public void disable() throws DurableDataLogException {
        // Not implemented.
    }

    @Override
    public CompletableFuture<Void> truncate(LogAddress upToAddress, Duration timeout) {
        // Not implemented.
        return CompletableFuture.completedFuture(null);
    }


}
