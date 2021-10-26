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

import com.google.common.base.Preconditions;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import io.pravega.logstore.shared.protocol.Request;
import io.pravega.logstore.shared.protocol.RequestProcessor;
import io.pravega.logstore.shared.protocol.commands.AbstractCommand;
import io.pravega.logstore.shared.protocol.commands.ReleasableCommand;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConnectionInboundHandler extends ChannelInboundHandlerAdapter implements Connection {
    @Setter
    private volatile RequestProcessor processor = null;
    private volatile Channel channel = null;
    @Getter
    private volatile boolean closed = false;

    //region Connection Implementation

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        this.channel = ctx.channel();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Request request = (Request) msg;
        log.debug("Received request: {}", request);

        try {
            request.process(getProcessor());
        } catch (Throwable ex) {
            // Release buffers in case of an unhandled exception.
            if (request instanceof ReleasableCommand) {
                ((ReleasableCommand) request).release(); // Idempotent. Invoking multiple times has no side effects.
            }
            throw ex;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        logError(cause);
        ctx.close();
    }

    @Override
    public void send(AbstractCommand reply) {
        Channel c = getChannel();
        // Work around for https://github.com/netty/netty/issues/3246
        EventLoop eventLoop = c.eventLoop();
        eventLoop.execute(() -> write(c, reply));
    }

    @Override
    public void close() {
        if (!this.closed) {
            Channel ch = this.channel;
            if (ch != null) {
                // wait for all messages to be sent before closing the channel.
                ch.eventLoop().execute(() -> ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE));
            }

            RequestProcessor rp = this.processor;
            if (rp != null) {
                rp.close();
            }
            this.closed = true;
        }
    }

    @Override
    public void pauseReading() {
        log.debug("Pausing reading from connection {}.", this);
        getChannel().config().setAutoRead(false);
    }

    @Override
    public void resumeReading() {
        log.debug("Resuming reading from connection {}.", this);
        getChannel().config().setAutoRead(true);
    }

    //endregion

    private static void write(Channel channel, AbstractCommand data) {
        channel.write(data).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
    }

    private RequestProcessor getProcessor() {
        RequestProcessor requestProcessor = this.processor;
        Preconditions.checkState(requestProcessor != null, "No command processor set for connection %s.", this);
        return requestProcessor;
    }

    private Channel getChannel() {
        Channel ch = this.channel;
        Preconditions.checkState(ch != null, "Connection not yet established.");
        return ch;
    }

    private void logError(Throwable cause) {
        log.error("Caught exception on connection {}: ", this, cause);
    }

    @Override
    public String toString() {
        Channel c = this.channel;
        if (c == null) {
            return "NewServerConnection";
        }
        return c.toString();
    }
}
