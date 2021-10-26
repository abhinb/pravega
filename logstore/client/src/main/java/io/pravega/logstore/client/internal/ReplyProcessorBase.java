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

import io.pravega.common.Exceptions;
import io.pravega.logstore.client.internal.connections.ConnectionFailedException;
import io.pravega.logstore.shared.LogChunkExistsException;
import io.pravega.logstore.shared.LogChunkNotExistsException;
import io.pravega.logstore.shared.protocol.ReplyProcessor;
import io.pravega.logstore.shared.protocol.commands.AbstractCommand;
import io.pravega.logstore.shared.protocol.commands.ChunkAlreadyExists;
import io.pravega.logstore.shared.protocol.commands.ChunkNotExists;
import io.pravega.logstore.shared.protocol.commands.ErrorMessage;
import io.pravega.logstore.shared.protocol.commands.Hello;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
abstract class ReplyProcessorBase implements ReplyProcessor {
    private final String traceLogId;
    private final Consumer<Throwable> errorCallback;

    @Override
    public void connectionDropped() {
        failConnection(new ConnectionFailedException(String.format("Connection dropped for %s.", traceLogId)));
    }

    @Override
    public void hello(Hello hello) {
        if (hello.getLowVersion() > AbstractCommand.WIRE_VERSION || hello.getHighVersion() < AbstractCommand.OLDEST_COMPATIBLE_VERSION) {
            log.error("Incompatible wire protocol versions {}", hello);
        } else {
            log.info("Received hello: {}", hello);
        }
    }

    @Override
    public void chunkAlreadyExists(ChunkAlreadyExists alreadyExists) {
        log.info("{}: Log Chunk Replica already exists.", traceLogId);
        this.errorCallback.accept(new LogChunkExistsException(alreadyExists.getChunkId()));
    }

    @Override
    public void chunkNotExists(ChunkNotExists notExists) {
        log.info("{}: Log Chunk Replica does not exist.", traceLogId);
        this.errorCallback.accept(new LogChunkNotExistsException(notExists.getChunkId()));
    }

    @Override
    public void error(ErrorMessage errorMessage) {
        log.info("{}: General error {}: {}.", traceLogId, errorMessage.getErrorCode(), errorMessage.getMessage());
        this.errorCallback.accept(errorMessage.getThrowableException());
    }

    @Override
    public void processingFailure(Exception error) {
        failConnection(error);
    }

    protected void failConnection(Throwable e) {
        log.warn("{}: Failing connection with exception {}", traceLogId, e.toString());
        this.errorCallback.accept(Exceptions.unwrap(e));
    }
}
