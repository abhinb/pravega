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
package io.pravega.logstore.server;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NonNull;

@Getter
public final class ChunkEntry {
    private final long chunkId;
    private final long entryId;
    private final int crc32;
    private final ByteBuf data;
    private final int length;

    public ChunkEntry(long chunkId, long entryId, int crc32, @NonNull ByteBuf data) {
        Preconditions.checkArgument(chunkId >= 0, "ChunkId must be non-negative.");
        Preconditions.checkArgument(entryId >= 0, "EntryId must be non-negative.");
        this.chunkId = chunkId;
        this.entryId = entryId;
        this.crc32 = crc32;
        this.data = data;
        this.length = data.readableBytes(); // This may change as we read the buffer. Remember it for future use.
    }

    @Override
    public String toString() {
        return String.format("%s-%s [%s] %s bytes", this.chunkId, this.entryId, this.crc32, getLength());
    }
}
