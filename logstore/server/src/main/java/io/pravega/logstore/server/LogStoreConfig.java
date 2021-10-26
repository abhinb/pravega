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
package io.pravega.logstore.server;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.nio.file.Path;
import lombok.Getter;
import lombok.SneakyThrows;

@Getter
public class LogStoreConfig {
    public static final Property<String> LISTENING_IP_ADDRESS = Property.named("service.listener.host.nameOrIp", "");
    public static final Property<Integer> LISTENING_PORT = Property.named("service.listener.port", 12345);

    public static final Property<String> STORAGE_PATH = Property.named("storage.path", "/tmp/pravega/logstore");
    public static final Property<Integer> CORE_POOL_SIZE = Property.named("threadpool.core.size", 8);
    public static final Property<Integer> WRITE_POOL_SIZE = Property.named("threadpool.write.size", 8);
    public static final Property<Integer> READ_POOL_SIZE = Property.named("threadpool.read.size", 4);
    public static final Property<Integer> MAX_QUEUE_READ_COUNT = Property.named("writer.queue.read.size.max", 10);
    public static final Property<Integer> WRITE_BLOCK_SIZE = Property.named("writer.block.size.bytes", 4 * 1024 * 1024);
    public static final Property<Integer> READ_BLOCK_SIZE = Property.named("reader.block.size.bytes", 4 * 1024 * 1024);
    public static final Property<Integer> MAX_READ_SIZE = Property.named("reader.max.size.bytes", 12 * 1024 * 1024);

    public static final String COMPONENT_CODE = "logstore";

    private static final String DATA_FILE_PATH_TEMPLATE = "%s.data";
    private static final String INDEX_FILE_PATH_TEMPLATE = "%s.index";
    private static final String METADATA_FILE_PATH_TEMPLATE = "%s.metadata";

    /**
     * The TCP Port number to listen to.
     */
    private final int listeningPort;

    /**
     * The IP address to listen to.
     */
    private final String listeningIPAddress;

    private final String storagePath;
    private final int corePoolSize;
    private final int writePoolSize;
    private final int readPoolSize;
    private final int maxQueueReadCount;
    private final int writeBlockSize;
    private final int readBlockSize;
    private final int maxReadSize;

    private LogStoreConfig(TypedProperties properties) {
        this.listeningPort = properties.getInt(LISTENING_PORT);
        String ipAddress = properties.get(LISTENING_IP_ADDRESS);
        if (ipAddress == null || ipAddress.equals(LISTENING_IP_ADDRESS.getDefaultValue())) {
            // Can't put this in the 'defaultValue' above because that would cause getHostAddress to be evaluated every time.
            ipAddress = getHostAddress();
        }
        this.listeningIPAddress = ipAddress;
        this.storagePath = properties.get(STORAGE_PATH); // Not checked whether a valid path; we do that elsewhere.
        this.corePoolSize = properties.getPositiveInt(CORE_POOL_SIZE);
        this.writePoolSize = properties.getPositiveInt(WRITE_POOL_SIZE);
        this.readPoolSize = properties.getPositiveInt(READ_POOL_SIZE);
        this.maxQueueReadCount = properties.getPositiveInt(MAX_QUEUE_READ_COUNT);
        this.writeBlockSize = properties.getPositiveInt(WRITE_BLOCK_SIZE);
        this.readBlockSize = properties.getPositiveInt(READ_BLOCK_SIZE);
        this.maxReadSize = properties.getPositiveInt(MAX_READ_SIZE);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<LogStoreConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, LogStoreConfig::new);
    }

    @SneakyThrows(UnknownHostException.class)
    private static String getHostAddress() {
        return Inet4Address.getLocalHost().getHostAddress();
    }

    public Path getChunkReplicaDataFilePath(long chunkId) {
        return Path.of(this.storagePath, String.format(DATA_FILE_PATH_TEMPLATE, chunkId));
    }

    public Path getChunkReplicaIndexFilePath(long chunkId) {
        return Path.of(this.storagePath, String.format(INDEX_FILE_PATH_TEMPLATE, chunkId));
    }

    public Path getChunkReplicaMetadataFilePath(long chunkId) {
        return Path.of(this.storagePath, String.format(METADATA_FILE_PATH_TEMPLATE, chunkId));
    }
}

