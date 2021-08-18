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
package io.pravega.logstore.shared.protocol.commands;

import com.google.common.base.Preconditions;
import io.pravega.logstore.shared.protocol.EnhancedByteBufInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;

public enum CommandType {
    // Admin.
    HELLO(-127, Hello::readFrom),
    ERROR_MESSAGE(-126, Hello::readFrom),
    KEEP_ALIVE(-125, KeepAlive::readFrom),

    // Control path.
    CREATE_CHUNK(1, CreateChunk::readFrom),
    CHUNK_CREATED(2, ChunkCreated::readFrom),
    CHUNK_ALREADY_EXISTS(3, ChunkAlreadyExists::readFrom),
    CHUNK_NOT_EXISTS(4, ChunkNotExists::readFrom),

    SEAL_CHUNK(5, SealChunk::readFrom),
    CHUNK_SEALED(6, ChunkSealed::readFrom),

    // Data Path.
    APPEND_ENTRY(10, AppendEntry::readFrom),
    ENTRY_APPENDED(11, EntryAppended::readFrom),
    BAD_ENTRY_ID(12, BadEntryId::readFrom),

    READ_ENTRIES(13, ReadEntries::readFrom),
    ENTRIES_READ(14, EntriesRead::readFrom),

    GET_CHUNK_INFO(15, GetChunkInfo::readFrom),
    CHUNK_INFO(16, ChunkInfo::readFrom);

    private static final Map<Integer, CommandType> MAPPING;
    @Getter
    private final int code;
    private final AbstractCommand.Constructor factory;

    CommandType(int code, AbstractCommand.Constructor factory) {
        Preconditions.checkArgument(code <= Byte.MAX_VALUE && code > Byte.MIN_VALUE, "Invalid CommandCode %s.", code);
        this.code = code;
        this.factory = factory;
    }

    public AbstractCommand readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
        return factory.readFrom(in, length);
    }

    static {
        HashMap<Integer, CommandType> map = new HashMap<>();
        for (CommandType t : CommandType.values()) {
            map.put(t.getCode(), t);
        }
        MAPPING = Collections.unmodifiableMap(map);
    }

    public static CommandType fromCode(int code) {
        return MAPPING.get(code);
    }

}
