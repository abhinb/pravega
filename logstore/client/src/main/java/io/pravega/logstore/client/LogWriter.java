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
package io.pravega.logstore.client;

import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;

public interface LogWriter extends AutoCloseable {
    long getLogId();

    LogInfo getInfo();

    CompletableFuture<Void> initialize();

    CompletableFuture<EntryAddress> append(@NonNull ByteBuf data);

    QueueStatistics getQueueStatistics();

    LogReader getReader(); // A bit odd, but we only allow reading owned logs (do not support reads while writing from other clients).

    @Override
    void close();
}
