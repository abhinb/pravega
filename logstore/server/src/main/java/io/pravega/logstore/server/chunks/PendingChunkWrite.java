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
import io.pravega.logstore.server.ChunkEntry;
import lombok.Getter;
import lombok.Setter;

@Getter
class PendingChunkWrite implements PendingChunkOp {
    private final long entryId;
    private volatile ByteBuf data;
    @Setter
    private volatile long offset = -1L;

    PendingChunkWrite(ChunkEntry entry, DataFormat dataFormat) {
        this.entryId = entry.getEntryId();
        this.data = dataFormat.serialize(entry);
    }

    @Override
    public boolean isTerminal() {
        return this.data == null;
    }

    @Override
    public boolean hasCompletion() {
        return false;
    }

    @Override
    public void complete() {
        // This method intentionally left blank.
    }

    @Override
    public void fail(Throwable exception) {
        // This method intentionally left blank.
    }

    void slice(int fromOffset) {
        this.data = sliceFrom(fromOffset);
    }

    private ByteBuf sliceFrom(int offset) {
        return this.data.slice(offset, this.data.readableBytes() - offset);
    }

    boolean hasData() {
        return this.data.readableBytes() > 0;
    }

    @Override
    public String toString() {
        return String.format("EntryId=%s, Offset=%s, Length=%s", this.entryId, this.offset, this.data.readableBytes());
    }

}