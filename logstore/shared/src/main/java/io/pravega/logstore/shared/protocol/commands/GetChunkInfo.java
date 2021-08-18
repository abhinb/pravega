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
import io.pravega.logstore.shared.protocol.Request;
import io.pravega.logstore.shared.protocol.RequestProcessor;
import java.io.DataOutput;
import java.io.IOException;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class GetChunkInfo extends AbstractCommand implements Request {
    final CommandType type = CommandType.GET_CHUNK_INFO;
    final long requestId;
    final long chunkId;

    @Override
    public void writeFields(DataOutput out) throws IOException {
        out.writeLong(this.requestId);
        out.writeLong(this.chunkId);
    }

    public static AbstractCommand readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
        long requestId = in.readLong();
        long chunkId = in.readLong();
        return new GetChunkInfo(requestId, chunkId);
    }

    @Override
    public void process(RequestProcessor processor) {
        processor.getChunkInfo(this);
    }
}
