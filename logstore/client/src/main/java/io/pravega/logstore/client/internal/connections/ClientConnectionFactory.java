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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.logstore.shared.protocol.CommandDecoder;
import io.pravega.logstore.shared.protocol.CommandEncoder;
import io.pravega.logstore.shared.protocol.ExceptionLoggingHandler;
import io.pravega.logstore.shared.protocol.ReplyProcessor;
import io.pravega.logstore.shared.protocol.commands.AbstractCommand;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ClientConnectionFactory implements AutoCloseable {

    private static final AtomicInteger POOLCOUNT = new AtomicInteger();

    private final EventLoopGroup group;
    private final ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    @Getter
    private final ScheduledExecutorService internalExecutor;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ClientConnectionFactory(int numThreadsInPool) {
        this.group = getEventLoopGroup();
        this.internalExecutor = ExecutorServiceHelpers.newScheduledThreadPool(numThreadsInPool,
                "clientInternal-" + POOLCOUNT.incrementAndGet());
    }

    public CompletableFuture<ClientConnection> establishConnection(URI endpoint, ReplyProcessor rp) {
        final ClientConnectionAdapter adapter = new ClientConnectionAdapter("test", rp);
        final Bootstrap b = new Bootstrap()
                .group(group)
                .channel(Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(getChannelInitializer(adapter));

        // Initiate Connection.
        final CompletableFuture<ClientConnection> connectionComplete = new CompletableFuture<>();
        try {
            b.connect(endpoint.getHost(), endpoint.getPort()).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    //since ChannelFuture is complete future.channel() is not a blocking call.
                    Channel ch = future.channel();
                    log.debug("Connect operation completed for channel:{}, local address:{}, remote address:{}",
                            ch.id(), ch.localAddress(), ch.remoteAddress());
                    channelGroup.add(ch); // Once a channel is closed the channel group implementation removes it.

                    connectionComplete.complete(new NettyConnection(ch, adapter));
                } else {
                    connectionComplete.completeExceptionally(new ConnectionFailedException(future.cause()));
                }
            });
        } catch (Throwable e) {
            connectionComplete.completeExceptionally(new ConnectionFailedException(e));
        }

        return connectionComplete;
    }

    private ChannelInitializer<SocketChannel> getChannelInitializer(ChannelInboundHandlerAdapter adapter) {
        return new ChannelInitializer<>() {
            @Override
            public void initChannel(SocketChannel ch) {
                ChannelPipeline p = ch.pipeline();
                p.addLast(
                        new ExceptionLoggingHandler(),
                        new CommandEncoder(),
                        new LengthFieldBasedFrameDecoder(AbstractCommand.MAX_COMMAND_SIZE, 4, 4),
                        new CommandDecoder(),
                        adapter);
            }
        };
    }

    private EventLoopGroup getEventLoopGroup() {
        if (Epoll.isAvailable()) {
            return new EpollEventLoopGroup();
        } else {
            log.warn("Epoll not available. Falling back on NIO.");
            return new NioEventLoopGroup();
        }
    }

    @Override
    public void close() {
        log.info("Shutting down connection factory");
        if (closed.compareAndSet(false, true)) {
            ExecutorServiceHelpers.shutdown(internalExecutor);
        }
    }
}