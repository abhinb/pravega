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
package io.pravega.logstore.shared.protocol.commands;

import io.pravega.logstore.shared.protocol.EnhancedByteBufInputStream;
import io.pravega.logstore.shared.protocol.Reply;
import io.pravega.logstore.shared.protocol.ReplyProcessor;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.val;

@Data
@EqualsAndHashCode(callSuper = false)
public class EntriesRead extends ReleasableCommand implements Reply {
    final CommandType type = CommandType.ENTRIES_READ;
    final long requestId;
    final long chunkId;
    final List<EntryData> entries;

    @Override
    public void writeFields(DataOutput out) throws IOException {
        out.writeLong(this.requestId);
        out.writeLong(this.chunkId);
        out.writeInt(this.entries.size());
        for (EntryData e : this.entries) {
            e.writeTo(out);
        }
    }

    public static AbstractCommand readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
        long requestId = in.readLong();
        long chunkId = in.readLong();
        int count = in.readInt();
        val entries = new ArrayList<EntryData>();
        for (int i = 0; i < count; i++) {
            entries.add(EntryData.readFrom(in));
        }
        return new EntriesRead(requestId, chunkId, entries).requireRelease();
    }

    @Override
    void releaseInternal() {
        this.entries.forEach(EntryData::release);
    }

    @Override
    public long getRequestId() {
        return 0;
    }

    @Override
    public void process(ReplyProcessor processor) {
        processor.entriesRead(this);
    }
}
