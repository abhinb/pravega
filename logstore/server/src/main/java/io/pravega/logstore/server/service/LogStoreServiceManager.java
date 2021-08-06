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
package io.pravega.logstore.server.service;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;

public class LogStoreServiceManager implements AutoCloseable {
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(45);
    private final ApplicationConfig applicationConfig;
    private final ScheduledExecutorService coreExecutor;
    private final ScheduledExecutorService writeExecutor;
    private final ScheduledExecutorService readExecutor;
    @Getter
    private final LogStoreService service;

    @Builder
    private LogStoreServiceManager(@NonNull ApplicationConfig applicationConfig) {
        this.applicationConfig = applicationConfig;
        val logStoreConfig = this.applicationConfig.getConfig(LogStoreConfig::builder);
        this.coreExecutor = ExecutorServiceHelpers.newScheduledThreadPool(logStoreConfig.getCorePoolSize(), "core");
        this.writeExecutor = ExecutorServiceHelpers.newScheduledThreadPool(logStoreConfig.getCorePoolSize(), "write");
        this.readExecutor = ExecutorServiceHelpers.newScheduledThreadPool(logStoreConfig.getCorePoolSize(), "read");
        this.service = new LogStoreService(logStoreConfig, this.coreExecutor, this.writeExecutor, this.readExecutor);
    }

    public static LogStoreServiceManager.LogStoreServiceManagerBuilder inMemoryBuilder(ApplicationConfig config) {
        return builder().applicationConfig(config);
    }

    @Override
    public void close() {
        ExecutorServiceHelpers.shutdown(this.coreExecutor, this.writeExecutor, this.readExecutor);
    }
}
