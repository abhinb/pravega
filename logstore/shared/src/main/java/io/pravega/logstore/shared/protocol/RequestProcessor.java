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
package io.pravega.logstore.shared.protocol;

import io.pravega.logstore.shared.protocol.commands.AppendEntry;
import io.pravega.logstore.shared.protocol.commands.CreateChunk;
import io.pravega.logstore.shared.protocol.commands.GetChunkInfo;
import io.pravega.logstore.shared.protocol.commands.Hello;
import io.pravega.logstore.shared.protocol.commands.KeepAlive;
import io.pravega.logstore.shared.protocol.commands.ReadEntries;
import io.pravega.logstore.shared.protocol.commands.SealChunk;

public interface RequestProcessor extends AutoCloseable {

    @Override
    void close();

    void hello(Hello hello);

    void keepAlive(KeepAlive keepAlive);

    void createChunk(CreateChunk request);

    void sealChunk(SealChunk request);

    void appendEntry(AppendEntry request);

    void readEntries(ReadEntries request);

    void getChunkInfo(GetChunkInfo request);
}
