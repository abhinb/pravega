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

import lombok.Builder;
import lombok.NonNull;

@Builder
public class LogStoreServiceManager implements AutoCloseable {
    @NonNull
    private final ApplicationConfig applicationConfig;

    public static LogStoreServiceManager.LogStoreServiceManagerBuilder inMemoryBuilder(ApplicationConfig config) {
        return builder().applicationConfig(config);
    }

    @Override
    public void close() {

    }

    public void initialize() {

    }

    public LogStoreService createService() {
        return null;
    }
}
