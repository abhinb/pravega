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
package io.pravega.test.integration.selftest.adapters;

import com.google.common.base.Preconditions;
import io.netty.buffer.Unpooled;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.lang.ProcessStarter;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.Property;
import io.pravega.logstore.client.LogClient;
import io.pravega.logstore.client.LogClientConfig;
import io.pravega.logstore.client.LogWriter;
import io.pravega.logstore.server.LogStoreConfig;
import io.pravega.logstore.server.LogStoreServiceStarter;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.test.integration.selftest.Event;
import io.pravega.test.integration.selftest.TestConfig;
import io.pravega.test.integration.selftest.TestLogger;
import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.curator.test.TestingServer;

public class LogStoreAdapter extends StoreAdapter {
    //region Members

    private final TestConfig testConfig;
    private final ScheduledExecutorService executor;
    private final ConcurrentHashMap<String, LogWriter> writers;
    private final TestingServer zkServer;
    private final Thread stopServerProcess;
    private Process serverProcess;
    private final LogClient logClient;
    private final URI serverURI;

    //endregion

    //region Constructor

    @SneakyThrows
    LogStoreAdapter(TestConfig testConfig, ScheduledExecutorService executor) {
        this.testConfig = Preconditions.checkNotNull(testConfig, "testConfig");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        Preconditions.checkArgument(testConfig.getBookieCount() == 1, "LogStoreAdapter requires exactly one Bookie (LogStore instance).");
        this.serverURI = URI.create(String.format("tcp://%s:%s", getHostAddress(), this.testConfig.getBkPort(0)));
        this.writers = new ConcurrentHashMap<>();
        this.stopServerProcess = new Thread(this::stopServer);
        Runtime.getRuntime().addShutdownHook(this.stopServerProcess);
        val clientConfig = LogClientConfig.builder()
                .replicationFactor(1)
                .clientThreadPoolSize(10)
                .zkURL("localhost:" + testConfig.getZkPort())
                .build();
        this.zkServer = new TestingServer(testConfig.getZkPort());
        this.logClient = new LogClient(clientConfig, Collections.singletonList(this.serverURI));
    }

    //endregion

    //region StoreAdapter Implementation.

    @Override
    public boolean isFeatureSupported(Feature feature) {
        return feature == Feature.CreateStream
                || feature == Feature.Append;
    }

    @Override
    protected void startUp() throws Exception {
        // Start ZK.
        this.zkServer.start();

        // Start LogStore.
        this.serverProcess = startServer(this.testConfig, this.serverURI, "test");
    }

    @Override
    @SneakyThrows
    protected void shutDown() {
        this.writers.values().forEach(LogWriter::close);
        this.writers.clear();
        this.logClient.close();
        this.zkServer.stop();
        this.zkServer.close();
        stopServer();
        Runtime.getRuntime().removeShutdownHook(this.stopServerProcess);
        this.stopServerProcess.interrupt();

    }

    @Override
    public CompletableFuture<Void> createStream(String logName, Duration timeout) {
        ensureRunning();

        val writer = this.logClient.createLogWriter(logName.hashCode());
        this.writers.put(logName, writer);
        try {
            val result = writer.initialize();
            Futures.exceptionListener(result, ex -> {
                this.writers.remove(logName);
                writer.close();
            });
            return result;
        } catch (Throwable ex) {
            this.writers.remove(logName);
            writer.close();
            throw ex;
        }
    }

    @Override
    public CompletableFuture<Void> append(String logName, Event event, Duration timeout) {
        ensureRunning();
        LogWriter writer = this.writers.getOrDefault(logName, null);
        if (writer == null) {
            return Futures.failedFuture(new StreamSegmentNotExistsException(logName));
        }

        ArrayView s = event.getSerialization();
        return Futures.toVoid(writer.append(Unpooled.wrappedBuffer(s.array(), s.arrayOffset(), s.getLength())));
    }

    @Override
    public StoreReader createReader() {
        throw new UnsupportedOperationException("createReader() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<String> createTransaction(String parentStream, Duration timeout) {
        throw new UnsupportedOperationException("createTransaction() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout) {
        throw new UnsupportedOperationException("mergeTransaction() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<Void> abortTransaction(String transactionName, Duration timeout) {
        throw new UnsupportedOperationException("abortTransaction() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<Void> sealStream(String streamName, Duration timeout) {
        throw new UnsupportedOperationException("seal() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<Void> deleteStream(String streamName, Duration timeout) {
        throw new UnsupportedOperationException("delete() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<Void> createTable(String tableName, Duration timeout) {
        throw new UnsupportedOperationException("createTable() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<Void> deleteTable(String tableName, Duration timeout) {
        throw new UnsupportedOperationException("deleteTable() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<Long> updateTableEntry(String tableName, BufferView key, BufferView value, Long compareVersion, Duration timeout) {
        throw new UnsupportedOperationException("updateTableEntry() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<Void> removeTableEntry(String tableName, BufferView key, Long compareVersion, Duration timeout) {
        throw new UnsupportedOperationException("removeTableEntry() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<List<BufferView>> getTableEntries(String tableName, List<BufferView> keys, Duration timeout) {
        throw new UnsupportedOperationException("getTableEntry() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<AsyncIterator<List<Map.Entry<BufferView, BufferView>>>> iterateTableEntries(String tableName, Duration timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExecutorServiceHelpers.Snapshot getStorePoolSnapshot() {
        return null;
    }

    //endregion

    private void stopServer() {
        val process = this.serverProcess;
        if (process != null) {
            process.destroyForcibly();
            log("LogStore Service shut down.");
            this.serverProcess = null;
        }
    }

    /**
     * Starts a BookKeeper (using a number of bookies) along with a ZooKeeper out-of-process.
     *
     * @param config The Test Config to use. This indicates the BK Port(s), ZK Port, as well as Bookie counts.
     * @param logId  A String to use for logging purposes.
     * @return A Process referring to the newly started Bookie process.
     * @throws IOException If an error occurred.
     */
    static Process startServer(TestConfig config, URI serverURI, String logId) throws IOException {
        val port = config.getBkPort(0);
        Process p = ProcessStarter
                .forClass(LogStoreServiceStarter.class)
                .sysProp(configProperty(LogStoreConfig.LISTENING_IP_ADDRESS), serverURI.getHost())
                .sysProp(configProperty(LogStoreConfig.LISTENING_PORT), serverURI.getPort())
                .sysProp(configProperty(LogStoreConfig.STORAGE_PATH), LogStoreConfig.STORAGE_PATH.getDefaultValue() + "/" + port)
                .stdOut(ProcessBuilder.Redirect.to(new File(config.getComponentOutLogPath("logstore", 0))))
                .stdErr(ProcessBuilder.Redirect.to(new File(config.getComponentErrLogPath("logstore", 0))))
                .start();
        Exceptions.handleInterrupted(() -> Thread.sleep(2000));
        TestLogger.log(logId, "LogStore Service (Port %s) started.", config.getBkPort(0));
        return p;
    }

    private static String configProperty(Property<?> property) {
        return String.format("%s.%s", LogStoreConfig.COMPONENT_CODE, property.getName());
    }

    @SneakyThrows
    static String getHostAddress() {
        return Inet4Address.getLocalHost().getHostAddress();
    }
}
