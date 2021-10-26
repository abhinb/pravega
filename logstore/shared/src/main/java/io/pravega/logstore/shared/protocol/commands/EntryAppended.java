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

import io.netty.buffer.ByteBufInputStream;
import io.pravega.logstore.shared.protocol.Reply;
import io.pravega.logstore.shared.protocol.ReplyProcessor;
import java.io.DataOutput;
import java.io.IOException;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class EntryAppended extends AbstractCommand implements Reply {
    final CommandType type = CommandType.ENTRY_APPENDED;
    final long chunkId;
    final long upToEntryId;

    @Override
    public void writeFields(DataOutput out) throws IOException {
        out.writeLong(chunkId);
        out.writeLong(upToEntryId);
    }

    public static AbstractCommand readFrom(ByteBufInputStream in, int length) throws IOException {
        long requestId = in.readLong();
        long upToEntryId = in.readLong();
        return new EntryAppended(requestId, upToEntryId);
    }

    @Override
    public void process(ReplyProcessor cp) {
        cp.entryAppended(this);
    }

    @Override
    public long getRequestId() {
        return 0;
    }
}
