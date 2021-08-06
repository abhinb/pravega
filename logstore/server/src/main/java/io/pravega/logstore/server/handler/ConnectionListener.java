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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.pravega.common.Exceptions;
import io.pravega.logstore.server.service.LogStoreService;
import io.pravega.logstore.shared.protocol.CommandDecoder;
import io.pravega.logstore.shared.protocol.CommandEncoder;
import io.pravega.logstore.shared.protocol.RequestProcessor;
import io.pravega.logstore.shared.protocol.commands.AbstractCommand;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConnectionListener implements AutoCloseable {

    private final String host;
    private final int port;
    private final LogStoreService service;
    private Channel serverChannel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public ConnectionListener(@NonNull String host, int port, @NonNull LogStoreService service) {
        this.host = host;
        this.port = port;
        this.service = service;
    }

    @Override
    public void close() {
        // Wait until the server socket is closed.
        Channel sc = this.serverChannel;
        if (sc != null) {
            sc.close();
            Exceptions.handleInterrupted(sc.closeFuture()::sync);
        }

        // Shut down all event loops to terminate all threads.
        EventLoopGroup bg = this.bossGroup;
        if (bg != null) {
            bg.shutdownGracefully();
        }
        EventLoopGroup wg = this.workerGroup;
        if (wg != null) {
            wg.shutdownGracefully();
        }
    }

    /**
     * Initializes the connection listener internals and starts listening.
     */
    public void startListening() {
        boolean nio = false;
        try {
            this.bossGroup = new EpollEventLoopGroup(1);
            this.workerGroup = new EpollEventLoopGroup();
        } catch (ExceptionInInitializerError | UnsatisfiedLinkError | NoClassDefFoundError e) {
            nio = true;
            this.bossGroup = new NioEventLoopGroup(1);
            this.workerGroup = new NioEventLoopGroup();
        }

        ServerBootstrap b = new ServerBootstrap();
        b.group(this.bossGroup, this.workerGroup)
                .channel(nio ? NioServerSocketChannel.class : EpollServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 100)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();

                        // Configure the class-specific encoder stack and request processors.
                        ConnectionInboundHandler handler = new ConnectionInboundHandler();
                        createEncodingStack(ch.remoteAddress().toString(), p);
                        handler.setProcessor(createRequestProcessor(handler));
                        p.addLast(handler);
                    }
                });

        // Start the server.
        this.serverChannel = b.bind(this.host, this.port).awaitUninterruptibly().channel();
    }

    private void createEncodingStack(String connectionName, ChannelPipeline p) {
        p.addLast(new ExceptionLoggingHandler(connectionName));
        p.addLast(new CommandEncoder());
        p.addLast(new LengthFieldBasedFrameDecoder(AbstractCommand.MAX_COMMAND_SIZE, 4, 4));
        p.addLast(new CommandDecoder());
    }

    private RequestProcessor createRequestProcessor(Connection connection) {
        return new LogStoreRequestProcessor(this.service, connection);
    }
}
