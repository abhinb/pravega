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
import java.util.concurrent.CompletableFuture;
import lombok.Data;
import lombok.NonNull;

@Data
public class PendingAddEntry {
    private final long chunkId;
    private final long entryId;
    private final int crc32;
    @NonNull
    private final ByteBuf data;
    private final CompletableFuture<Void> completion = new CompletableFuture<>();

    @Override
    public String toString() {
        return String.format("LogChunkId=%s, EntryId=%s, Crc=%s, Length=%s, Status=%s", this.chunkId, this.entryId, this.crc32,
                this.data.readableBytes(), this.completion.isDone() ? (this.completion.isCompletedExceptionally() ? "Error" : "Done") : "Pending");
    }
}
