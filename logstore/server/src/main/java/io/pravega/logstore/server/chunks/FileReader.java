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
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@RequiredArgsConstructor
@Slf4j
class FileReader implements AutoCloseable {
    @Getter
    private final Path path;
    @GuardedBy("lock")
    private FileChannel channel;
    @GuardedBy("lock")
    @Getter
    private volatile long length;
    private final Object lock = new Object();

    @Override
    public void close() throws IOException {
        synchronized (this.lock) {
            if (this.channel != null) {
                this.channel.close();
                this.channel = null;
            }
        }
    }

    public void open() throws IOException {
        synchronized (this.lock) {
            Preconditions.checkState(this.channel == null, "'%s' is already open.", this);
            File f = this.path.toFile();
            if (!f.exists()) {
                throw new FileNotFoundException(this.path.toString());
            }
            this.length = f.length();
            this.channel = FileChannel.open(path, StandardOpenOption.READ);
        }
    }

    public ByteBuf read(long offset, int length) throws IOException {
        synchronized (this.lock) {
            Preconditions.checkArgument(offset >= 0 && length >= 0 && offset + length <= this.length,
                    "offset(%s) and length(%s) are invalid for the current file length(%s).", offset, length, this.length);
            val result = Unpooled.buffer(length);
            while (length > 0) {
                assert result.writableBytes() == length;
                int bytesRead = result.writeBytes(this.channel, offset, length);
                log.debug("{}: Read {} bytes from offset {}.", this.path, bytesRead, offset);
                assert bytesRead > 0 : "unable to make read progress";
                offset += bytesRead;
                length -= bytesRead;
            }
            return result;
        }
    }

    @Override
    public String toString() {
        return String.format("%s", this.path);
    }

}
