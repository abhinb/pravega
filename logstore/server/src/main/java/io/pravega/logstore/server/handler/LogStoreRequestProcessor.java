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
package io.pravega.logstore.server.handler;

import io.pravega.common.tracing.TagLogger;
import io.pravega.logstore.server.service.LogStoreService;
import io.pravega.logstore.shared.protocol.RequestProcessor;
import io.pravega.logstore.shared.protocol.commands.AppendEntry;
import io.pravega.logstore.shared.protocol.commands.CreateChunk;
import io.pravega.logstore.shared.protocol.commands.Hello;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.slf4j.LoggerFactory;

@RequiredArgsConstructor
public class LogStoreRequestProcessor implements RequestProcessor {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(LogStoreRequestProcessor.class));
    @NonNull
    private final LogStoreService service;
    @NonNull
    private final Connection connection;

    @Override
    public void close() {
        // Cleanup.
    }

    @Override
    public void hello(@NonNull Hello hello) {

    }

    @Override
    public void createChunk(@NonNull CreateChunk request) {

    }

    @Override
    public void appendEntry(@NonNull AppendEntry request) {

    }
}
