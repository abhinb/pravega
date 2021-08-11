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
package io.pravega.logstore.server.chunks;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class AppendOnlyFileWriter implements AutoCloseable {
    @Getter
    private final Path path;
    private final boolean sync;
    @GuardedBy("lock")
    private FileChannel channel;
    @Getter
    @GuardedBy("lock")
    private volatile long length;
    private final Object lock = new Object();

    protected AppendOnlyFileWriter(@NonNull Path path, boolean sync) {
        this.path = path;
        this.sync = sync;
        this.length = 0L;
        this.channel = null; // We require open() to be invoked.
    }

    @Override
    public void close() throws IOException {
        synchronized (this.lock) {
            if (this.channel != null) {
                flush();
                this.channel.close();
                this.channel = null;
            }
        }
    }

    @Override
    public String toString() {
        return String.format("%s [%s]", this.path, this.sync ? "SYNC" : "NON-SYNC");
    }

    public void open() throws IOException {
        synchronized (this.lock) {
            Preconditions.checkState(this.channel == null, "'%s' is already open.", this);
            this.channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        }
    }

    public void append(ByteBuf data, long offset) throws IOException {
        final int count = data.readableBytes();
        if (count == 0) {
            // Nothing to do.
            return;
        }

        synchronized (this.lock) {
            Preconditions.checkState(this.channel != null, "'%s' is not open.", this);
            if (offset != this.length) {
                throw new BadOffsetException(this.path, this.length, offset);
            }

            while (data.readableBytes() > 0) {
                // Copy data from the buffer to the channel. No need to invoke force() here, as we have opened the
                // channel with appropriate options.
                int bytesWritten = data.readBytes(this.channel, this.length, data.readableBytes());
                assert bytesWritten > 0 : "unable to make write progress";
                log.debug("{}: Wrote {} bytes at offset {} ({} remaining).", this, this.length, bytesWritten, data.readableBytes());
            }

            if (this.sync) {
                this.channel.force(true);
            }
            this.length += count;
        }
    }

    public void flush() throws IOException {
        synchronized (this.lock) {
            Preconditions.checkState(this.channel != null, "'%s' is not open.", this);
            this.channel.force(true);
        }
    }

    public static final class BadOffsetException extends RuntimeException {
        private BadOffsetException(Path path, long expectedOffset, long actualOffset) {
            super(String.format("Bad Offset for '%s'. Expected %s, given %s.", path, expectedOffset, actualOffset));
        }
    }
}
