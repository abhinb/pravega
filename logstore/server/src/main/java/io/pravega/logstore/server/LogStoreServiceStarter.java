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

import io.pravega.common.Exceptions;
import io.pravega.logstore.server.handler.ConnectionListener;
import io.pravega.logstore.server.service.ApplicationConfig;
import io.pravega.logstore.server.service.LogStoreConfig;
import io.pravega.logstore.server.service.LogStoreService;
import io.pravega.logstore.server.service.LogStoreServiceManager;
import java.util.concurrent.atomic.AtomicReference;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class LogStoreServiceStarter {
    private final ApplicationConfig appConfig;
    private final LogStoreConfig serviceConfig;
    private volatile LogStoreServiceManager serviceManager;
    private volatile ConnectionListener listener;
    private volatile boolean closed;

    public LogStoreServiceStarter(@NonNull ApplicationConfig config) {
        this.appConfig = config;
        this.serviceConfig = this.appConfig.getConfig(LogStoreConfig::builder);
    }

    private LogStoreServiceManager createServiceManager() {
        val builder = LogStoreServiceManager.inMemoryBuilder(this.appConfig);
        // TODO add real storage adapter, ZK, etc..
        return builder.build();
    }

    void start() {
        Exceptions.checkNotClosed(this.closed, this);
        log.info("Initializing Service Manager ...");
        this.serviceManager = createServiceManager();

        this.listener = new ConnectionListener(this.serviceConfig.getListeningIPAddress(), this.serviceConfig.getListeningPort(), this.serviceManager.getService());
        this.listener.startListening();
        log.info("ConnectionListener started successfully. Listening on {}:{}.", this.serviceConfig.getListeningIPAddress(), this.serviceConfig.getListeningPort());
        log.info("Log Store Service started.");
    }

    void shutdown() {
        if (!this.closed) {
            close(this.listener, "Connection Listener");
            close(this.serviceManager, "Log Store Service");
            this.closed = true;
        }
    }

    @SneakyThrows
    void close(AutoCloseable c, String name) {
        if (c != null) {
            c.close();
            log.info("{} closed.", name);
        }
    }

    //region main()

    public static void main(String[] args) {
        val serviceStarter = new AtomicReference<LogStoreServiceStarter>();
        try {
            System.err.println(System.getProperty(ApplicationConfig.CONFIG_FILE_PROPERTY_NAME, "config.properties"));
            // Load up the AppConfig, using this priority order (lowest to highest):
            // 1. Configuration file (either default or specified via SystemProperties)
            // 2. System Properties overrides (these will be passed in via the command line or inherited from the JVM)
            ApplicationConfig config = ApplicationConfig
                    .builder()
                    .include(System.getProperty(ApplicationConfig.CONFIG_FILE_PROPERTY_NAME, "config.properties"))
                    .include(System.getProperties())
                    .build();

            // For debugging purposes, it may be useful to know the non-default values for configurations being used.
            // This will unfortunately include all System Properties as well, but knowing those can be useful too sometimes.
            log.info("LogStore configuration:");
            config.forEach((key, value) -> log.info("{} = {}", key, value));
            serviceStarter.set(new LogStoreServiceStarter(config));
        } catch (Throwable e) {
            log.error("Could not create a Service with default config, Aborting.", e);
            System.exit(1);
        }

        try {
            serviceStarter.get().start();
        } catch (Throwable e) {
            log.error("Could not start the Service, Aborting.", e);
            System.exit(1);
        }

        try {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Caught interrupt signal...");
                serviceStarter.get().shutdown();
            }));

            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException ex) {
            log.info("Caught interrupt signal...");
        } finally {
            serviceStarter.get().shutdown();
            System.exit(0);
        }
    }

    //endregion
}
