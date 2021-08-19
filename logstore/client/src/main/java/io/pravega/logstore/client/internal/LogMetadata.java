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
package io.pravega.logstore.client.internal;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.val;

@Getter
@Builder
class LogMetadata extends VersionedMetadata {
    static final Serializer SERIALIZER = new Serializer();
    private static final String PATH = "LogMetadata";
    private final long logId;
    private final long epoch;
    private final List<ChunkMetadata> chunks;

    String getPath() {
        return getPath(this.logId);
    }

    static String getPath(long logId) {
        return String.format("/%s/%s", PATH, logId);
    }

    static LogMetadata empty(long logId) {
        return new LogMetadata(logId, 0L, Collections.emptyList());
    }

    LogMetadata addChunk(long chunkId, Collection<URI> locations) {
        val newChunks = new ArrayList<>(this.chunks);
        newChunks.add(new ChunkMetadata(chunkId, locations));
        val result = new LogMetadata(this.logId, this.epoch, newChunks);
        result.setVersion(this.getVersion());
        return result;
    }

    LogMetadata newEpoch() {
        val result = new LogMetadata(this.logId, this.epoch + 1, this.chunks);
        result.setVersion(this.getVersion());
        return result;
    }

    public static class LogMetadataBuilder implements ObjectBuilder<LogMetadata> {

    }

    @Data
    static class ChunkMetadata {
        private final long chunkId;
        private final Collection<URI> locations;
    }

    static class Serializer extends VersionedSerializer.WithBuilder<LogMetadata, LogMetadata.LogMetadataBuilder> {
        @Override
        protected LogMetadata.LogMetadataBuilder newBuilder() {
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

        private void write00(LogMetadata e, RevisionDataOutput target) throws IOException {
            target.writeLong(e.logId);
            target.writeLong(e.epoch);
            target.writeCollection(e.chunks, this::writeChunk00);
        }

        private void read00(RevisionDataInput source, LogMetadata.LogMetadataBuilder b) throws IOException {
            b.logId(source.readLong());
            b.epoch(source.readLong());
            b.chunks(source.readCollection(this::readChunk00, ArrayList::new));
        }

        private void writeChunk00(RevisionDataOutput target, ChunkMetadata c) throws IOException {
            target.writeLong(c.chunkId);
            target.writeCollection(c.locations, (t, uri) -> t.writeUTF(uri.toString()));
        }

        private ChunkMetadata readChunk00(RevisionDataInput source) throws IOException {
            val chunkId = source.readLong();
            val locations = source.readCollection(s -> URI.create(s.readUTF()), ArrayList::new);
            return new ChunkMetadata(chunkId, locations);
        }
    }
}
