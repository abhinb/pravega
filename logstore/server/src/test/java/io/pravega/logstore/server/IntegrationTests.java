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
import io.pravega.logstore.client.LogClient;
import io.pravega.logstore.client.LogClientConfig;
import io.pravega.logstore.client.internal.EntryAddress;
import io.pravega.logstore.client.internal.LogChunkReplicaWriterImpl;
import io.pravega.logstore.client.internal.LogChunkWriterImpl;
import io.pravega.logstore.client.internal.LogServerManager;
import io.pravega.logstore.client.internal.PendingAddEntry;
import io.pravega.logstore.client.internal.connections.ClientConnectionFactory;
import io.pravega.logstore.server.service.ApplicationConfig;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class IntegrationTests {
    private static final URI LOCAL_URI_1 = URI.create("tcp://127.0.1.1:12345");
    private static final URI LOCAL_URI_2 = URI.create("tcp://127.0.1.1:12346");
    private static final URI LOCAL_URI_3 = URI.create("tcp://127.0.1.1:12347");
    private static final LogServerManager LOG_SERVER_MANAGER = new LogServerManager(
            Arrays.asList(LOCAL_URI_1, LOCAL_URI_2, LOCAL_URI_3));
    private static final int ZK_PORT = 2181;
    private static final LogClientConfig CLIENT_CONFIG = LogClientConfig.builder()
            .replicationFactor(1)
            .clientThreadPoolSize(4)
            .rolloverSizeBytes(10 * 1024 * 1024)
            .zkURL("localhost:" + ZK_PORT)

            .build();

    private List<LogStoreServiceStarter> services;
    private TestingServer zkServer;

    @Before
    public void setup() throws Exception {
        this.zkServer = new TestingServer(ZK_PORT, true);
        log.info("ZK Server started at port {}.", ZK_PORT);
        this.services = LOG_SERVER_MANAGER.getServerUris().stream()
                .map(uri -> {
                    val appConfig = ApplicationConfig.builder()
                            .include(LogStoreConfig.builder()
                                    .with(LogStoreConfig.LISTENING_IP_ADDRESS, uri.getHost())
                                    .with(LogStoreConfig.LISTENING_PORT, uri.getPort())
                                    .with(LogStoreConfig.STORAGE_PATH, LogStoreConfig.STORAGE_PATH.getDefaultValue() + "/" + uri.getPort()))
                            .build();
                    return new LogStoreServiceStarter(appConfig);
                })
                .collect(Collectors.toList());
        this.services.forEach(LogStoreServiceStarter::start);
        log.info("All Services Started.");
    }

    @After
    public void tearDown() throws Exception {
        this.services.forEach(LogStoreServiceStarter::shutdown);
        if (this.zkServer != null) {
            this.zkServer.stop();
            this.zkServer.close();
        }
    }

    @Test
    public void testLogWriter() throws Exception {
        final int count = 200;
        final int writeSize = 1000000;
        final long logId = 0L;
        @Cleanup
        val client = new LogClient(CLIENT_CONFIG, LOG_SERVER_MANAGER);
        log.info("Created Client");
        @Cleanup
        val writer = client.createLogWriter(logId);
        log.info("Created Writer");
        val init = writer.initialize();
        init.join();
        log.info("Initialized Writer");

        val futures = new ArrayList<CompletableFuture<EntryAddress>>();
        val latencies = Collections.synchronizedList(new ArrayList<Integer>());
        val rnd = new Random(0);
        val data = new byte[writeSize];
        rnd.nextBytes(data);
        val timer = new Timer();
        for (int i = 0; i < count; i++) {
            val startTimeNanos = timer.getElapsedNanos();
            val f = writer.append(Unpooled.wrappedBuffer(data));
            futures.add(f);
            f.thenAccept(address -> {
                val elapsed = timer.getElapsedNanos() - startTimeNanos;
                latencies.add((int) (elapsed / AbstractTimer.NANOS_TO_MILLIS));
                log.debug("    Entry {} acked.", address);
            });
            f.join(); // todo enable for latency test; disable for tput test
        }

        val writeSendTime = timer.getElapsedMillis();
        log.info("Wrote {} entries.", count);

        Futures.allOf(futures).join();
        val writeAckTime = timer.getElapsedMillis();
        log.info("All entries acked.");

        if (latencies.size() > 0) {
            val avgLatency = latencies.stream().mapToInt(i -> i).average().orElse(0);
            val maxLatency = latencies.stream().mapToInt(i -> i).max().orElse(0);
            latencies.sort(Integer::compareTo);
            System.err.println(String.format("RESULT: WriteSend: %s ms, WriteAck: %s ms, L_avg: %.1f, L50: %s, L90: %s, L99: %s, L_max: %s",
                    writeSendTime, writeAckTime, avgLatency,
                    latencies.get(latencies.size() / 2),
                    latencies.get((int) (latencies.size() * 0.9)),
                    latencies.get((int) (latencies.size() * 0.99)),
                    maxLatency));
        }
    }

    @Test
    public void testLogChunkWriter() throws Exception {
        final int count = 1000;
        final int writeSize = 1000000;
        final int replicaCount = 3;
        final long logId = 0L;
        final long chunkId = 0L;

        @Cleanup
        val factory = new ClientConnectionFactory(4);
        log.info("Created Client Factory");
        @Cleanup
        val writer = new LogChunkWriterImpl(logId, chunkId, replicaCount, LOG_SERVER_MANAGER, factory.getInternalExecutor());
        log.info("Created Writer");
        val init = writer.initialize(factory);
        init.join();
        log.info("Initialized Writer");

        val futures = new ArrayList<CompletableFuture<Void>>();
        val latencies = Collections.synchronizedList(new ArrayList<Integer>());
        val rnd = new Random(0);
        val data = new byte[writeSize];
        rnd.nextBytes(data);
        val timer = new Timer();
        for (int i = 0; i < count; i++) {
            val startTimeNanos = timer.getElapsedNanos();
            val e = new PendingAddEntry(Unpooled.wrappedBuffer(data), i * i);
            e.setWriter(writer);
            e.setEntryId(i);
            val f = writer.addEntry(e);
            futures.add(f);
            f.thenRun(() -> {
                val elapsed = timer.getElapsedNanos() - startTimeNanos;
                latencies.add((int) (elapsed / AbstractTimer.NANOS_TO_MILLIS));
                log.debug("    Entry {} acked.", e.getEntryId());
                e.close();
            });
            //f.join(); // todo enable for latency test; disable for tput test
        }

        val writeSendTime = timer.getElapsedMillis();
        log.info("Wrote {} entries.", count);

        Futures.allOf(futures).join();
        val writeAckTime = timer.getElapsedMillis();
        log.info("All entries acked.");

        val avgLatency = latencies.stream().mapToInt(i -> i).average().getAsDouble();
        val maxLatency = latencies.stream().mapToInt(i -> i).max().getAsInt();
        latencies.sort(Integer::compareTo);
        System.err.println(String.format("RESULT: WriteSend: %s ms, WriteAck: %s ms, L_avg: %.1f, L50: %s, L90: %s, L99: %s, L_max: %s",
                writeSendTime, writeAckTime, avgLatency,
                latencies.get(latencies.size() / 2),
                latencies.get((int) (latencies.size() * 0.9)),
                latencies.get((int) (latencies.size() * 0.99)),
                maxLatency));

    }

    @Test
    public void testLogChunkReplicaWriter() throws Exception {
        final int count = 1000;
        final int writeSize = 1000000;
        final long logId = 0L;
        final long chunkId = 0L;
        @Cleanup
        val factory = new ClientConnectionFactory(4);
        log.info("Created Client Factory");
        @Cleanup
        val writer = new LogChunkReplicaWriterImpl(logId, chunkId, LOCAL_URI_1, factory.getInternalExecutor());
        log.info("Created Writer");
        val init = writer.initialize(factory);
        init.join();
        log.info("Initialized Writer");

        val futures = new ArrayList<CompletableFuture<Void>>();
        val latencies = Collections.synchronizedList(new ArrayList<Integer>());
        val rnd = new Random(0);
        val data = new byte[writeSize];
        rnd.nextBytes(data);
        val timer = new Timer();
        for (int i = 0; i < count; i++) {
            val startTimeNanos = timer.getElapsedNanos();
            val e = new PendingAddEntry(Unpooled.wrappedBuffer(data), i * i);
            e.setWriter(writer);
            e.setEntryId(i);
            val f = writer.addEntry(e);
            futures.add(f);
            f.thenRun(() -> {
                val elapsed = timer.getElapsedNanos() - startTimeNanos;
                latencies.add((int) (elapsed / AbstractTimer.NANOS_TO_MILLIS));
                log.debug("    Entry {} acked.", e.getEntryId());
                e.close();
            });
            f.join(); // todo
        }

        val writeSendTime = timer.getElapsedMillis();
        log.info("Wrote {} entries.", count);

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
