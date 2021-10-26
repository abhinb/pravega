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

import io.pravega.logstore.shared.protocol.Reply;
import io.pravega.logstore.shared.protocol.ReplyProcessor;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class BadEntryId extends AbstractCommand implements Reply {
    final CommandType type = CommandType.BAD_ENTRY_ID;
    final long requestId;
    final long chunkId;
    final long expectedEntryId;
    final long actualEntryId;

    @Override
    public void process(ReplyProcessor cp) {
        cp.badEntryId(this);
    }

    @Override
    public void writeFields(DataOutput out) throws IOException {
        out.writeLong(this.requestId);
        out.writeLong(this.chunkId);
        out.writeLong(this.expectedEntryId);
        out.writeLong(this.actualEntryId);
    }

    public static AbstractCommand readFrom(DataInput in, int length) throws IOException {
        long requestId = in.readLong();
        long chunkId = in.readLong();
        long expectedEntryId = in.readLong();
        long actualEntryId = in.readLong();
        return new BadEntryId(requestId, chunkId, expectedEntryId, actualEntryId);
    }
}