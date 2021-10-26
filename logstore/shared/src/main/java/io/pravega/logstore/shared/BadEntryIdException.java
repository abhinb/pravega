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
package io.pravega.logstore.shared;

import lombok.Getter;

@Getter
public class BadEntryIdException extends RuntimeException {
    private final long chunkId;
    private final long expectedEntryId;
    private final long providedEntryId;

    BadEntryIdException(long chunkId, long expectedEntryId, long providedEntryId) {
        super(String.format("Bad Entry Id for Log Chunk %s. Expected %s, given %s.", chunkId, expectedEntryId, providedEntryId));
        this.chunkId = chunkId;
        this.expectedEntryId = expectedEntryId;
        this.providedEntryId = providedEntryId;
    }
}
