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

import io.pravega.logstore.shared.protocol.EnhancedByteBufInputStream;
import io.pravega.logstore.shared.protocol.Reply;
import io.pravega.logstore.shared.protocol.ReplyProcessor;
import java.io.DataOutput;
import java.io.IOException;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * A generic error response that encapsulates an error code (to be used for client-side processing) and an error message
 * describing the origin of the error. This should be used to describe general exceptions where limited information is required.
 */
@Data
@EqualsAndHashCode(callSuper = false)
public final class ErrorMessage extends AbstractCommand implements Reply {
    final CommandType type = CommandType.ERROR_MESSAGE;
    final long requestId;
    final long chunkId;
    final String errorCode;
    final String message;

    @Override
    public void process(ReplyProcessor cp) throws UnsupportedOperationException {
        cp.error(this);
    }

    @Override
    public void writeFields(DataOutput out) throws IOException {
        out.writeLong(requestId);
        out.writeLong(this.chunkId);
        out.writeUTF(errorCode == null ? "" : errorCode);
        out.writeUTF(message == null ? "" : message);
    }

    public static AbstractCommand readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
        return new ErrorMessage(in.readLong(), in.readLong(), in.readUTF(), in.readUTF());
    }

    public RuntimeException getThrowableException() {
        return new RuntimeException(String.format("%s: %s.", getErrorCode(), getMessage()));
    }

    @Override
    public boolean isFailure() {
        return true;
    }
}