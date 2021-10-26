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

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.logstore.server.ChunkInfo;
import java.io.IOException;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ChunkMetadata implements ChunkInfo {
    public static final Serializer SERIALIZER = new Serializer();
    private final long chunkId;
    private final long entryCount;
    private final long dataLength;
    private final long indexLength;

    static class ChunkMetadataBuilder implements ObjectBuilder<ChunkMetadata> {
    }

    static class Serializer extends VersionedSerializer.WithBuilder<ChunkMetadata, ChunkMetadataBuilder> {
        static final int MAX_SERIALIZATION_SIZE = 1024; // Upper bound, not exact.

        @Override
        protected ChunkMetadataBuilder newBuilder() {
            return builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(ChunkMetadata chunkMetadata, RevisionDataOutput out) throws IOException {
            out.writeLong(chunkMetadata.chunkId);
            out.writeLong(chunkMetadata.entryCount);
            out.writeLong(chunkMetadata.dataLength);
            out.writeLong(chunkMetadata.indexLength);
        }

        private void read00(RevisionDataInput in, ChunkMetadataBuilder b) throws IOException {
            b.chunkId(in.readLong());
            b.entryCount(in.readLong());
            b.dataLength(in.readLong());
            b.indexLength(in.readLong());
        }
    }
}
