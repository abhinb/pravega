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
import io.pravega.common.AbstractTimer;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.logstore.client.internal.LogChunkReplicaWriterImpl;
import io.pravega.logstore.client.internal.PendingAddEntry;
import io.pravega.logstore.client.internal.connections.ClientConnectionFactory;
import io.pravega.logstore.server.service.ApplicationConfig;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
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
        final int count = 1000;
        final int writeSize = 1000000;
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

        val entries = new ArrayList<PendingAddEntry>();
        val latencies = Collections.synchronizedList(new ArrayList<Integer>());
        val rnd = new Random(0);
        val data = new byte[writeSize];
        rnd.nextBytes(data);
        val timer = new Timer();
        for (int i = 0; i < count; i++) {
            val startTimeNanos = timer.getElapsedNanos();
            val e = new PendingAddEntry(chunkId, i, i * i, Unpooled.wrappedBuffer(data));
            writer.addEntry(e);
            entries.add(e);
            e.getCompletion().thenRun(() -> {
                val elapsed = timer.getElapsedNanos() - startTimeNanos;
                latencies.add((int) (elapsed / AbstractTimer.NANOS_TO_MILLIS));
                log.debug("    Entry {} acked.", e.getEntryId());
            });
            e.getCompletion().join();//todo
        }

        val writeSendTime = timer.getElapsedMillis();
        log.info("Wrote {} entries.", count);

        val futures = entries.stream().map(PendingAddEntry::getCompletion).collect(Collectors.toList());
        Futures.allOf(futures).join();
        val writeAckTime = timer.getElapsedMillis();
        log.info("All entries acked.");
        //Thread.sleep(5000);// TODO

        val avgLatency = latencies.stream().mapToInt(i -> i).average().getAsDouble();
        val maxLatency = latencies.stream().mapToInt(i -> i).max().getAsInt();
        latencies.sort(Integer::compareTo);
        System.out.println(String.format("RESULT: WriteSend: %s ms, WriteAck: %s ms, L_avg: %.1f, L50: %s, L90: %s, L99: %s, L_max: %s",
                writeSendTime, writeAckTime, avgLatency,
                latencies.get(latencies.size() / 2),
                latencies.get((int) (latencies.size() * 0.9)),
                latencies.get((int) (latencies.size() * 0.99)),
                maxLatency));

    }
}
