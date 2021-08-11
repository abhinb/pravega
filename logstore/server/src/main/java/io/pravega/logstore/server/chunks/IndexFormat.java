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
import java.util.List;
import lombok.val;

class IndexFormat {
    private static final int SINGLE_ENTRY_LENGTH = Long.BYTES * 2; // Entry Id -> Offset.

    public ByteBuf serialize(List<PendingWrite> entryWrites) {
        int size = SINGLE_ENTRY_LENGTH * entryWrites.size();
        val result = Unpooled.buffer(size);
        int offset = 0;
        for (val e : entryWrites) {
            result.setLong(offset, e.getEntryId());
            offset += Long.BYTES;
            result.setLong(offset, e.getOffset());
            offset += Long.BYTES;
        }

        return result.writerIndex(size).readerIndex(0);
    }
}
