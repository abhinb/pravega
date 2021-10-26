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
package io.pravega.logstore.server.handler;

import io.pravega.logstore.shared.protocol.RequestProcessor;
import io.pravega.logstore.shared.protocol.commands.AbstractCommand;

public interface Connection extends AutoCloseable {

    /**
     * Sends the provided {@link AbstractCommand} asynchronously. This operation is non-blocking.
     *
     * @param reply The {@link AbstractCommand} to send.
     */
    void send(AbstractCommand reply);

    /**
     * Sets the command processor to receive incoming commands from the client. This
     * method may only be called once.
     *
     * @param rp The Request Processor to set.
     */
    void setProcessor(RequestProcessor rp);

    void pauseReading();

    void resumeReading();

    /**
     * Drop the connection. No further operations may be performed.
     */
    @Override
    void close();

    /**
     * Checks if this instance is already closed.
     *
     * @return {@code true} if this object is already closed, otherwise returns {@code false}.
     */
    boolean isClosed();
}
