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
import java.util.ArrayList;
import lombok.Getter;

@Getter
class WriteBatch {
    private final long offset;
    private int remainingCapacity;
    private int length;
    private final ArrayList<ByteBuf> buffers = new ArrayList<>();
    private final ArrayList<PendingChunkWrite> writes = new ArrayList<>();

    WriteBatch(long offset, int maxSize) {
        this.offset = offset;
        this.remainingCapacity = maxSize;
        this.length = 0;
    }

    @Override
    public String toString() {
        return String.format("Offset=%s, Length=%s, EntryIds=[%s-%s]", this.offset, this.length, getFirstEntryId(), getLastEntryId());
    }

    boolean isEmpty() {
        return this.length == 0 && this.buffers.isEmpty() && this.writes.isEmpty();
    }

    boolean isFull() {
        return this.remainingCapacity <= 0;
    }

    Long getFirstEntryId() {
        return hasCompletedEntries() ? this.writes.get(0).getEntryId() : null;
    }

    Long getLastEntryId() {
        return hasCompletedEntries() ? this.writes.get(this.writes.size() - 1).getEntryId() : null;
    }

    boolean hasCompletedEntries() {
        return this.writes.size() > 0;
    }

    ByteBuf get() {
        return Unpooled.wrappedUnmodifiableBuffer(this.buffers.toArray(new ByteBuf[this.buffers.size()]));
    }

    void add(PendingChunkWrite write) {
        if (isFull()) {
            return;
        }

        ByteBuf buf;
        if (write.getData().readableBytes() <= this.remainingCapacity) {
            // Whole fit
            buf = write.getData();
            this.writes.add(write);
        } else {
            // Partial fit
            buf = write.getData().slice(0, this.remainingCapacity);
        }

        this.buffers.add(buf);
        this.remainingCapacity -= buf.readableBytes();
        this.length += buf.readableBytes();
        write.slice(buf.readableBytes());
    }
}
