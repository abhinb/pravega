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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.pravega.logstore.shared.protocol.commands.AbstractCommand;
import io.pravega.logstore.shared.protocol.commands.AppendEntry;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class NettyConnection implements ClientConnection {
    @Getter
    private final String connectionName;
    @VisibleForTesting
    @Getter
    private final ClientConnectionAdapter nettyHandler;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public NettyConnection(String connectionName, ClientConnectionAdapter nettyHandler) {
        this.connectionName = connectionName;
        this.nettyHandler = nettyHandler;
    }

    @Override
    public void send(AbstractCommand cmd) throws ConnectionFailedException {
        checkClientConnectionClosed();
        write(cmd);
    }

    @Override
    public void send(AppendEntry append) throws ConnectionFailedException {
        checkClientConnectionClosed();
        write(append);
    }

    private void write(AbstractCommand cmd) throws ConnectionFailedException {
        Channel channel = nettyHandler.getChannel();
        EventLoop eventLoop = channel.eventLoop();
        ChannelPromise promise = channel.newPromise();
        promise.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                nettyHandler.setRecentMessage();
                if (!future.isSuccess()) {
                    future.channel().pipeline().fireExceptionCaught(future.cause());
                }
            }
        });
        // Work around for https://github.com/netty/netty/issues/3246
        eventLoop.execute(() -> {
            try {
                if (!closed.get()) {
                    channel.write(cmd, promise);
                }
            } catch (Exception e) {
                channel.pipeline().fireExceptionCaught(e);
            }
        });
    }

    @Override
    public void sendAsync(AppendEntry cmd, CompletedCallback callback) {
        Channel channel = null;
        try {
            checkClientConnectionClosed();
            channel = nettyHandler.getChannel();
            log.debug("Write and flush message {} on channel {}", cmd, channel);
            channel.writeAndFlush(cmd)
                    .addListener((Future<? super Void> f) -> {
                        nettyHandler.setRecentMessage();
                        if (f.isSuccess()) {
                            callback.complete(null);
                        } else {
                            callback.complete(new ConnectionFailedException(f.cause()));
                        }
                    });
        } catch (ConnectionFailedException cfe) {
            log.debug("ConnectionFailedException observed when attempting to write WireCommand {} ", cmd);
            callback.complete(cfe);
        } catch (Exception e) {
            log.warn("Exception while attempting to write WireCommand {} on netty channel {}", cmd, channel);
            callback.complete(new ConnectionFailedException(e));
        }
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            nettyHandler.closeFlow(this);
        }
    }

    private void checkClientConnectionClosed() throws ConnectionFailedException {
        if (closed.get()) {
            log.error("ClientConnection to {} with flow id disabled is already closed", connectionName);
            throw new ConnectionFailedException("Client connection already closed for flow disabled");
        }
    }

}
