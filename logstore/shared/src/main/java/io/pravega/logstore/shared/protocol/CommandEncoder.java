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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.pravega.logstore.shared.protocol.commands.AbstractCommand;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@NotThreadSafe
@Slf4j
public class CommandEncoder extends FlushingMessageToByteEncoder<Object> {
    private static final byte[] LENGTH_PLACEHOLDER = new byte[4];

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) {
        log.trace("Encoding message to send over the wire {}", msg);
        if (msg instanceof AbstractCommand) {
            writeMessage((AbstractCommand) msg, out);
            flushRequired();
        } else {
            throw new IllegalArgumentException("Expected an AbstractCommand and found: " + msg);
        }
    }

    @SneakyThrows(IOException.class)
    private int writeMessage(AbstractCommand msg, ByteBuf out) {
        int startIdx = out.writerIndex();
        ByteBufOutputStream bout = new ByteBufOutputStream(out);
        bout.writeInt(msg.getType().getCode());
        bout.write(LENGTH_PLACEHOLDER);
        msg.writeFields(bout);
        bout.flush();
        bout.close();
        int endIdx = out.writerIndex();
        int fieldsSize = endIdx - startIdx - AbstractCommand.TYPE_PLUS_LENGTH_SIZE;
        out.setInt(startIdx + AbstractCommand.TYPE_SIZE, fieldsSize);
        return endIdx - startIdx;
    }
}
