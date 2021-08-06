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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.pravega.logstore.shared.protocol.commands.AbstractCommand;
import io.pravega.logstore.shared.protocol.commands.CommandType;
import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import lombok.Cleanup;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Decodes commands coming over the wire.
 */
@Slf4j
@ToString
public class CommandDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        AbstractCommand command = parseCommand(in);
        if (log.isTraceEnabled()) {
            log.trace("Decode a message on connection: {}. Message was {}", ctx.channel().remoteAddress(), command);
        }
        if (command != null) {
            out.add(command);
        }
    }

    @VisibleForTesting
    public static AbstractCommand parseCommand(ByteBuf in) throws IOException {
        @Cleanup
        EnhancedByteBufInputStream is = new EnhancedByteBufInputStream(in);
        int readableBytes = in.readableBytes();
        if (readableBytes < AbstractCommand.TYPE_PLUS_LENGTH_SIZE) {
            throw new InvalidMessageException("Not enough bytes to read.");
        }
        CommandType type = readType(is);
        int length = readLength(is, readableBytes);
        int readIndex = in.readerIndex();
        AbstractCommand command = type.readFrom(is, length);
        in.readerIndex(readIndex + length);
        return command;
    }

    private static int readLength(DataInput is, int readableBytes) throws IOException {
        int length = is.readInt();
        if (length < 0) {
            throw new InvalidMessageException("Length read from wire was negative.");
        }
        if (length > readableBytes - AbstractCommand.TYPE_PLUS_LENGTH_SIZE) {
            throw new InvalidMessageException("Header indicated more bytes than exist.");
        }
        return length;
    }

    private static CommandType readType(DataInput is) throws IOException {
        int code = is.readInt();
        CommandType type = CommandType.fromCode(code);
        if (type == null) {
            throw new InvalidMessageException("Unknown wire command: " + code);
        }
        return type;
    }

}