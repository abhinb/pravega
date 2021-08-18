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

import io.netty.buffer.ByteBuf;
import io.pravega.logstore.shared.protocol.EnhancedByteBufInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import lombok.Data;

@Data
public class EntryData {
    private final long entryId;
    private final int crc32;
    private final ByteBuf data;

    void release() {
        this.data.release();
    }

    void writeTo(DataOutput out) throws IOException {
        out.writeLong(this.entryId);
        out.writeInt(this.crc32);
        out.writeInt(data.readableBytes());
        data.getBytes(data.readerIndex(), (OutputStream) out, data.readableBytes());
    }

    static EntryData readFrom(EnhancedByteBufInputStream in) throws IOException {
        long entryId = in.readLong();
        int crc32 = in.readInt();
        int dataLength = in.readInt();
        ByteBuf data = in.readFully(dataLength).retain();
        return new EntryData(entryId, crc32, data);
    }
}
