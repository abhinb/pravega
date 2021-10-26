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

import io.pravega.common.util.AsyncIterator;
import io.pravega.logstore.client.Entry;
import io.pravega.logstore.client.internal.connections.ClientConnectionFactory;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;

public interface LogChunkReader extends AsyncIterator<List<Entry>>, AutoCloseable {
    long getChunkId();

    long getEntryCount();

    long getLength();

    CompletableFuture<Void> initialize(@NonNull ClientConnectionFactory connectionFactory);

    Collection<URI> getReplicaURIs();

    @Override
    void close();
}
