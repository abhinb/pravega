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

import io.pravega.logstore.shared.protocol.commands.BadEntryId;
import io.pravega.logstore.shared.protocol.commands.ChunkAlreadyExists;
import io.pravega.logstore.shared.protocol.commands.ChunkCreated;
import io.pravega.logstore.shared.protocol.commands.ChunkNotExists;
import io.pravega.logstore.shared.protocol.commands.EntryAppended;
import io.pravega.logstore.shared.protocol.commands.ErrorMessage;
import io.pravega.logstore.shared.protocol.commands.Hello;

public interface ReplyProcessor {
    void hello(Hello hello);

    void error(ErrorMessage errorMessage);

    void chunkCreated(ChunkCreated reply);

    void chunkAlreadyExists(ChunkAlreadyExists reply);

    void chunkNotExists(ChunkNotExists reply);

    void entryAppended(EntryAppended entryAppended);

    void badEntryId(BadEntryId badEntryId);
}
