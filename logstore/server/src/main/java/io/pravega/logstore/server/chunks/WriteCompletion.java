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
package io.pravega.logstore.server.chunks;

import io.pravega.logstore.server.ChunkEntry;
import java.util.List;
import lombok.Data;

@Data
public final class WriteCompletion {
    private final long chunkId;
    private final List<ChunkEntry> entries;
    private final Throwable failure;

    public boolean isFailed() {
        return this.failure != null;
    }

    public boolean isEmpty() {
        return this.entries.size() == 0;
    }

    public long getFirstEntryId() {
        return isEmpty() ? -1L : this.entries.get(0).getEntryId();
    }

    public long getLastEntryId() {
        return isEmpty() ? -1L : this.entries.get(this.entries.size() - 1).getEntryId();
    }

    @Override
    public String toString() {
        return String.format("EntryId=%s-%s (%s). Failed=%s", getFirstEntryId(), getLastEntryId(), this.entries.size(), isFailed());
    }
}
