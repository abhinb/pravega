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

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import lombok.Getter;
import lombok.SneakyThrows;

@Getter
public class LogStoreConfig {
    public static final Property<String> LISTENING_IP_ADDRESS = Property.named("service.listener.host.nameOrIp", "");
    public static final Property<Integer> LISTENING_PORT = Property.named("service.listener.port", 12345);

    public static final Property<Integer> CORE_POOL_SIZE = Property.named("threadpool.core.size", 4);
    public static final Property<Integer> WRITE_POOL_SIZE = Property.named("threadpool.write.size", 16);
    public static final Property<Integer> READ_POOL_SIZE = Property.named("threadpool.read.size", 16);
    public static final String COMPONENT_CODE = "logstore";

    /**
     * The TCP Port number to listen to.
     */
    private final int listeningPort;

    /**
     * The IP address to listen to.
     */
    private final String listeningIPAddress;

    private final int corePoolSize;
    private final int writePoolSize;
    private final int readPoolSize;

    private LogStoreConfig(TypedProperties properties) {
        this.listeningPort = properties.getInt(LISTENING_PORT);
        String ipAddress = properties.get(LISTENING_IP_ADDRESS);
        if (ipAddress == null || ipAddress.equals(LISTENING_IP_ADDRESS.getDefaultValue())) {
            // Can't put this in the 'defaultValue' above because that would cause getHostAddress to be evaluated every time.
            ipAddress = getHostAddress();
        }
        this.listeningIPAddress = ipAddress;
        this.corePoolSize = properties.getPositiveInt(CORE_POOL_SIZE);
        this.writePoolSize = properties.getPositiveInt(WRITE_POOL_SIZE);
        this.readPoolSize = properties.getPositiveInt(READ_POOL_SIZE);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<LogStoreConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, LogStoreConfig::new);
    }

    @SneakyThrows(UnknownHostException.class)
    private static String getHostAddress() {
        return Inet4Address.getLocalHost().getHostAddress();
    }
}
