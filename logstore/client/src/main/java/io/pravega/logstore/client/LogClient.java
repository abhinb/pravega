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
package io.pravega.logstore.client;

import io.pravega.common.Exceptions;
import io.pravega.logstore.client.internal.LogServerManager;
import io.pravega.logstore.client.internal.LogWriterImpl;
import io.pravega.logstore.client.internal.connections.ClientConnectionFactory;
import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.NonNull;

public class LogClient implements AutoCloseable {
    private final ClientConnectionFactory connectionFactory;
    private final LogServerManager logServerManager;
    private final LogClientConfig config;
    private final AtomicBoolean closed;

    public LogClient(@NonNull LogClientConfig config, @NonNull List<URI> serverURIs) {
        this(config, new LogServerManager(serverURIs));
    }

    public LogClient(@NonNull LogClientConfig config, @NonNull LogServerManager logServerManager) {
        this.config = config;
        this.connectionFactory = new ClientConnectionFactory(config.getClientThreadPoolSize());
        this.logServerManager = logServerManager;
        this.closed = new AtomicBoolean(false);
    }

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.connectionFactory.close();
        }
    }

    public LogWriter createLogWriter(long logId) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return new LogWriterImpl(logId, this.config, this.logServerManager, this.connectionFactory);
    }
}
