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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.logstore.server.ChunkEntry;

class DataFormat {
    private static final int HEADER_LENGTH = Long.BYTES + 2 * Integer.BYTES; // Entry Id(Long), Length(Int), Crc32(Int)

    public ByteBuf serialize(ChunkEntry entry) {
        ByteBuf header = Unpooled.buffer(HEADER_LENGTH).writerIndex(0)
                .writeLong(entry.getEntryId())
                .writeInt(entry.getLength())
                .writeInt(entry.getCrc32());
        return Unpooled.wrappedUnmodifiableBuffer(header, entry.getData());
    }

    public ChunkEntry deserialize(ByteBuf buf, long chunkId) {
        if (buf.readableBytes() < HEADER_LENGTH) {
            return null;
        }

        long entryId = buf.readLong();
        int length = buf.readInt();
        int crc32 = buf.readInt();
        if (buf.readableBytes() < length) {
            // Not enough in buffer to keep entry; rollback and bail out.
            buf.readerIndex(buf.readerIndex() - HEADER_LENGTH);
            return null;
        } else {
            buf.readerIndex(buf.readerIndex() + length);
            return new ChunkEntry(chunkId, entryId, crc32, buf.slice(buf.readerIndex() - length, length));
        }
    }
}
