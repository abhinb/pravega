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
package io.pravega.logstore.client.internal.connections;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.pravega.common.util.ReusableFutureLatch;
import io.pravega.logstore.shared.protocol.Reply;
import io.pravega.logstore.shared.protocol.ReplyProcessor;
import io.pravega.logstore.shared.protocol.commands.AbstractCommand;
import io.pravega.logstore.shared.protocol.commands.Hello;
import io.pravega.logstore.shared.protocol.commands.KeepAlive;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class ClientConnectionAdapter extends ChannelInboundHandlerAdapter implements AutoCloseable {
    private static final int KEEP_ALIVE_TIMEOUT_SECONDS = 20;
    private final String connectionName;
    private volatile Channel channel;
    private final AtomicReference<ScheduledFuture<?>> keepAliveFuture = new AtomicReference<>();
    private final AtomicBoolean recentMessage = new AtomicBoolean(true);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final KeepAliveTask keepAlive = new KeepAliveTask();
    @Getter
    private final ReusableFutureLatch<Void> registeredFutureLatch = new ReusableFutureLatch<>();
    private final ReplyProcessor replyProcessor;

    private final AtomicBoolean disableFlow = new AtomicBoolean(false);

    public ClientConnectionAdapter(String connectionName, @NonNull ReplyProcessor replyProcessor) {
        this.connectionName = connectionName;
        this.replyProcessor = replyProcessor;
    }

    /**
     * Fetch the netty channel. If {@link Channel} is null then throw a ConnectionFailedException.
     *
     * @return The current {@link Channel}
     * @throws ConnectionFailedException Throw if connection is not established.
     */
    Channel getChannel() throws ConnectionFailedException {
        Channel ch = this.channel;
        if (ch == null) {
            throw new ConnectionFailedException("Connection to " + connectionName + " is not established.");
        }
        return ch;
    }

    /**
     * Set the Recent Message flag. This is used to detect connection timeouts.
     */
    void setRecentMessage() {
        recentMessage.set(true);
    }

    /**
     * This function completes the input future when the channel is ready.
     *
     * @param future CompletableFuture which will be completed once the channel is ready.
     */
    void completeWhenReady(final CompletableFuture<Void> future) {
        Preconditions.checkNotNull(future, "future");
        registeredFutureLatch.register(future);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        Channel ch = ctx.channel();
        this.channel = ch;
        log.info("Connection established with endpoint {} on channel {}", connectionName, ch);
        ch.writeAndFlush(new Hello(AbstractCommand.WIRE_VERSION, AbstractCommand.OLDEST_COMPATIBLE_VERSION), ch.voidPromise());
        registeredFutureLatch.release(null); //release all futures waiting for channel registration to complete.
        // WireCommands.KeepAlive messages are sent for every network connection to a SegmentStore.
        ScheduledFuture<?> old = keepAliveFuture.getAndSet(ch.eventLoop()
                .scheduleWithFixedDelay(keepAlive,
                        KEEP_ALIVE_TIMEOUT_SECONDS,
                        KEEP_ALIVE_TIMEOUT_SECONDS,
                        TimeUnit.SECONDS));
        if (old != null) {
            old.cancel(false);
        }
    }

    /**
     * Invoke all the {@link ReplyProcessor#connectionDropped()} for all the registered flows once the
     * connection is disconnected.
     *
     * @see io.netty.channel.ChannelInboundHandler#channelUnregistered(ChannelHandlerContext)
     */
    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        ScheduledFuture<?> future = keepAliveFuture.get();
        if (future != null) {
            future.cancel(false);
        }
        this.channel = null;
        log.info("Connection drop observed with endpoint {}", connectionName);
        val rp = this.replyProcessor;
        try {
            log.debug("Connection dropped.");
            invokeReply(ReplyProcessor::connectionDropped);
            rp.connectionDropped();
        } catch (Exception e) {
            // Suppressing exception which prevents all ReplyProcessor.connectionDropped from being invoked.
            log.warn("Encountered exception invoking ReplyProcessor ", e);
        }
        registeredFutureLatch.releaseExceptionally(new ConnectionClosedException());
        super.channelUnregistered(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Reply cmd = (Reply) msg;
        log.debug(connectionName + " processing reply {} with flow disabled", cmd);
        recentMessage.set(true);
        if (cmd instanceof Hello) {
            try {
                invokeReply(rp -> rp.hello((Hello) cmd));
            } catch (Exception e) {
                // Suppressing exception which prevents all ReplyProcessor.hello from being invoked.
                log.warn("Encountered exception invoking ReplyProcessor.hello", e);
            }
            return;
        }

        // Obtain ReplyProcessor and process the reply.
        try {
            invokeReply(rp -> rp.process(cmd));
        } catch (Exception e) {
            log.warn("ReplyProcessor.process failed for reply {} due to {}", cmd, e.getMessage());
            invokeReply(rp -> rp.processingFailure(e));
        }
    }

    private void invokeReply(Consumer<ReplyProcessor> handler) {
        val rp = this.replyProcessor;
        if (rp != null) {
            handler.accept(rp);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        invokeProcessingFailureForAllFlows(cause);
    }

    private void invokeProcessingFailureForAllFlows(Throwable cause) {
        try {
            log.debug("Exception observed due to {}", cause.getMessage());
            invokeReply(rp -> rp.processingFailure(new ConnectionFailedException(cause)));
        } catch (Exception e) {
            // Suppressing exception which prevents all ReplyProcessor.processingFailure from being invoked.
            log.warn("Encountered exception invoking ReplyProcessor.processingFailure.", e);
        }

    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            Channel ch = this.channel;
            if (ch != null) {
                this.channel = null;
                log.info("Closing connection with endpoint {} on channel {}", connectionName, ch);
                invokeProcessingFailureForAllFlows(new ConnectionClosedException());
                ch.close();
            }
        }
    }

    final class KeepAliveTask implements Runnable {
        @VisibleForTesting
        @Getter(AccessLevel.PACKAGE)
        private final ChannelFutureListener listener = new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                recentMessage.set(true);
                if (!future.isSuccess()) {
                    log.warn("Keepalive failed for connection {}", connectionName);
                    close();
                }
            }
        };

        @Override
        public void run() {
            if (recentMessage.getAndSet(false)) {
                try {
                    getChannel().writeAndFlush(new KeepAlive()).addListener(listener);
                } catch (Exception e) {
                    log.warn("Failed to send KeepAlive to {}. Closing this connection.", connectionName, e);
                    close();
                }
            } else {
                log.error("Connection {} stalled for more than {} seconds. Closing.", connectionName, KEEP_ALIVE_TIMEOUT_SECONDS);
                close();
            }
        }
    }
}
