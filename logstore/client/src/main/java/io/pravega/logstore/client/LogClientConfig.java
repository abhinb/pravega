/**
 * Copyright Pravega Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.logstore.client;

import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import io.pravega.common.util.ConfigBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class LogClientConfig {

    public static final Property<Integer> MAX_WRITE_SIZE_BYTES = Property.named("max.write.size.bytes", 1024 * 1024 - 1024);
    public static final Property<Integer> REPLICATION_FACTOR = Property.named("replicationFactor", 3);

    public static final Property<Long> ROLLOVER_SIZE_BYTES = Property.named("rollver.size.bytes", 1024L * 1024L * 1024L);

    public static final Property<Integer> MAX_WRITE_ATTEMPTS = Property.named("max.write.attempts", 10);

    public static final Property<Integer> WRITE_TIMEOUT_MILLIS = Property.named("write.timeout.millis", 30000);

    public static final Property<Integer> CLIENT_THREAD_POOL_SIZE = Property.named("client.thread.pool.size", 4);

    public static final Property<String> ZKURL = Property.named("zk.connect.uri", "localhost:2181");

    public static final Property<String> ZKPATH = Property.named("zk.path", "pravega/logstore");

    public static final Property<Integer> ZK_RETRY_SLEEP_MILLIS = Property.named("zk.retry.sleep.millis", 5000);

    public static final Property<Integer> ZK_RETRY_COUNT = Property.named("zk.retry.count", 10);

    public static final Property<Integer> ZK_SESSION_TIMEOUT_MILLIS = Property.named("zk.session.timeout.millis", 10000);

    public static final String COMPONENT_CODE = "logstoreclient";

    public static final int DEFAULT_MAX_WRITE_SIZE_BYTES = 1024 * 1024 - 1024;

    private final int replicationFactor;

    private final int maxWriteSizeBytes;

    private final long rolloverSizeBytes;

    private final int maxWriteAttempts;

    private final int writeTimeoutMillis;

    private final int clientThreadPoolSize;

    private final String zkURL;

    private final String zkPath;

    private final int zkRetrySleepMillis;

    private final int zkRetryCount;

    private final int zkSessionTimeoutMillis;

    private LogClientConfig(TypedProperties properties) {
        log.info("inside log client zk property is {}", properties.get(ZKURL));
        this.maxWriteSizeBytes = properties.getInt(MAX_WRITE_SIZE_BYTES);
        this.replicationFactor = properties.getInt(REPLICATION_FACTOR);
        this.rolloverSizeBytes = properties.getLong(ROLLOVER_SIZE_BYTES);
        this.maxWriteAttempts = properties.getInt(MAX_WRITE_ATTEMPTS);
        this.writeTimeoutMillis = properties.getInt(WRITE_TIMEOUT_MILLIS);
        this.clientThreadPoolSize = properties.getInt(CLIENT_THREAD_POOL_SIZE);
        this.zkURL = properties.get(ZKURL);
        this.zkPath = properties.get(ZKPATH);
        this.zkRetrySleepMillis = properties.getInt(ZK_RETRY_SLEEP_MILLIS);
        this.zkRetryCount = properties.getInt(ZK_RETRY_COUNT);
        this.zkSessionTimeoutMillis = properties.getInt(ZK_SESSION_TIMEOUT_MILLIS);
    }

    public static ConfigBuilder<LogClientConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, LogClientConfig::new);
    }
}
