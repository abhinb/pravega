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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public abstract class ReleasableCommand extends AbstractCommand {
    @Getter
    private boolean released = true;

    /**
     * Marks the fact that this instance requires {@link #release()} to be invoked in order to free up resources.
     *
     * @return This instance.
     */
    AbstractCommand requireRelease() {
        this.released = false;
        return this;
    }

    /**
     * Releases any resources used by this request, if needed {@code #isReleased()} is false. This method has no
     * effect if invoked multiple times or if no resource release is required.
     */
    public void release() {
        if (!this.released) {
            releaseInternal();
            this.released = true;
        }
    }

    /**
     * Internal implementation of {@link #release()}. Do not invoke directly as this method offers no protection
     * against multiple invocations.
     */
    abstract void releaseInternal();
}
