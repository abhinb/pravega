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

import io.pravega.common.util.BufferView;
import java.io.IOException;
import lombok.Getter;
import lombok.Setter;

abstract class VersionedMetadata {
    @Getter
    @Setter
    private volatile int version = -1;


    @FunctionalInterface
    interface Deserializer<R> {
        R apply(byte[] var1) throws IOException;
    }

    @FunctionalInterface
    interface Serializer<R> {
        BufferView apply(R var1) throws IOException;
    }
}
