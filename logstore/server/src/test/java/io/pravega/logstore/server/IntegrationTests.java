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

import io.netty.buffer.Unpooled;
import io.pravega.common.concurrent.Futures;
import io.pravega.logstore.client.internal.LogChunkReplicaWriterImpl;
import io.pravega.logstore.client.internal.PendingAddEntry;
import io.pravega.logstore.client.internal.connections.ClientConnectionFactory;
import io.pravega.logstore.server.service.ApplicationConfig;
import io.pravega.logstore.server.service.LogStoreConfig;
import java.net.URI;
import java.util.ArrayList;
import java.util.Random;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class IntegrationTests {
    private static final URI LOCAL_URI = URI.create("tcp://127.0.0.1:12345");
    private final ApplicationConfig appConfig = ApplicationConfig.builder()
            .include(LogStoreConfig.builder()
                    .with(LogStoreConfig.LISTENING_IP_ADDRESS, LOCAL_URI.getHost())
                    .with(LogStoreConfig.LISTENING_PORT, LOCAL_URI.getPort()))
            .build();

    private LogStoreServiceStarter service;

    @Before
    public void setup() {
        this.service = new LogStoreServiceStarter(appConfig);
        this.service.start();
    }

    @After
    public void tearDown() {
        val s = this.service;
        if (s != null) {
            s.shutdown();
        }
    }

    @Test
    public void testEndToEnd() throws Exception {
        final long chunkId = 0L;
        @Cleanup
        val factory = new ClientConnectionFactory(4);
        log.info("Created Client Factory");
        @Cleanup
        val writer = new LogChunkReplicaWriterImpl(chunkId, LOCAL_URI, factory.getInternalExecutor());
        log.info("Created Writer");
        val init = writer.initialize(factory);
        init.join();
        log.info("Initialized Writer");
        Thread.sleep(5000);

        val entries = new ArrayList<PendingAddEntry>();
        val rnd = new Random(0);
        for (int i = 0; i < 100; i++) {
            val data = new byte[100000];
            rnd.nextBytes(data);
            val e = new PendingAddEntry(chunkId, i, i * i, Unpooled.wrappedBuffer(data));
            writer.addEntry(e);
            entries.add(e);
            e.getCompletion().thenRun(() -> log.info("    Entry {} acked.", e.getEntryId()));
        }

        log.info("Wrote some entries.");

        val futures = entries.stream().map(PendingAddEntry::getCompletion).collect(Collectors.toList());
        Futures.allOf(futures).join();
        log.info("All entries acked.");

    }
}
