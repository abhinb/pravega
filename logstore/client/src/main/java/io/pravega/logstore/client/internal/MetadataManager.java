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
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.logstore.client.LogClientConfig;
import java.io.IOException;
import javax.annotation.concurrent.GuardedBy;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;


@Slf4j
@RequiredArgsConstructor
public class MetadataManager {
    static final String ZK_PATH = "/ClusterMetadata";
    private final CuratorFramework zkClient;
    private final LogClientConfig config;
    @GuardedBy("lock")
    private Metadata metadata;
    private final Object lock = new Object();


    public long getNextChunkId() {
        synchronized (this.lock) {
            val path = MetadataManager.ZK_PATH;
            for (int i = 0; i < this.config.getZkRetryCount(); i++) {
                this.metadata = get(path, Metadata.SERIALIZER::deserialize, new Metadata(0L));
                log.info("Metadata is assigned with version {} and chunkID {} in iteration {}", this.metadata.getVersion(), this.metadata.getNextChunkId(), i);
                this.metadata = this.metadata.withNextChunkId();
                if (set(this.metadata, path, Metadata.SERIALIZER::serialize)) {
                    log.info("return metadata with next chunk id with version {}  and id {}",this.metadata.getVersion(), this.metadata.getNextChunkId());
                    return this.metadata.getNextChunkId();
                } else {
                    log.debug("Conflict while trying to get next chunk id. Attempt {}/{}.", i + 1, this.config.getZkRetryCount());
                    this.metadata = get(path, Metadata.SERIALIZER::deserialize, this.metadata);
                    log.info("conflicted chunk with version {}  and id {}",this.metadata.getVersion(), this.metadata.getNextChunkId());
                }
            }

            log.warn("Unable to get next chunk id after {} attempts.", this.config.getZkRetryCount());
            throw new RetriesExhaustedException(new Exception("Unable to get next chunk id"));
        }
    }

    @SneakyThrows
    <T extends VersionedMetadata> T get(String path, VersionedMetadata.Deserializer<T> deserializer, T defaultValue) {
        try {
            Stat storingStatIn = new Stat();
            byte[] serialized = this.zkClient.getData().storingStatIn(storingStatIn).forPath(path);
            T result = deserializer.apply(serialized);
            result.setVersion(storingStatIn.getVersion());
            return result;
        } catch (KeeperException.NoNodeException nne) {
            // Node does not exist: this is the first time we are accessing this log.
            log.warn("No ZNode found for path '{}{}'. This is OK if this is the first time accessing this log.",
                    this.zkClient.getNamespace(), path);
            return defaultValue;
        }
    }

    @SneakyThrows
    <T extends VersionedMetadata> boolean set(T value, String path, VersionedMetadata.Serializer<T> serializer) {
        try {
            byte[] serialized = serializer.apply(value).getCopy();
            Stat result;
            if (value.getVersion() < 0) {
                result = new Stat();
                this.zkClient.create()
                        .creatingParentsIfNeeded()
                        .storingStatIn(result)
                        .forPath(path, serialized);
            } else {
                result = this.zkClient.setData()
                        .withVersion(value.getVersion())
                        .forPath(path, serialized);
            }
            value.setVersion(result.getVersion());
            log.info("Wrote data to ZK (Path={}{}, Version={}).", this.zkClient.getNamespace(), path, value.getVersion());
        } catch (KeeperException.NodeExistsException | KeeperException.BadVersionException keeperEx) {
            log.warn("Unable to write data to ZK due to version mismatch (Path={}{}, GivenVersion={}, Code={})).",
                    this.zkClient.getNamespace(), path, value.getVersion(), keeperEx.toString());
            return false;
        }

        return true;
    }


    @Builder
    @Getter
    private static class Metadata extends VersionedMetadata {
        private static final Serializer SERIALIZER = new Serializer();
        private final long nextChunkId;

        Metadata withNextChunkId() {
            val r = new Metadata(nextChunkId + 1);
            r.setVersion(this.getVersion());
            return r;
        }

        public static class MetadataBuilder implements ObjectBuilder<Metadata> {

        }

        private static class Serializer extends VersionedSerializer.WithBuilder<Metadata, Metadata.MetadataBuilder> {
            @Override
            protected Metadata.MetadataBuilder newBuilder() {
                return Metadata.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(Metadata e, RevisionDataOutput target) throws IOException {
                target.writeLong(e.nextChunkId);
            }

            private void read00(RevisionDataInput source, Metadata.MetadataBuilder b) throws IOException {
                b.nextChunkId(source.readLong());
            }
        }
    }


}
