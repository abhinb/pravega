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
package io.pravega.logstore.client.internal;

import com.google.common.base.Preconditions;
import java.net.URI;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import lombok.Getter;
import lombok.val;

public class LogServerManager {
    @Getter
    private final List<URI> serverUris;
    private final SecureRandom random = new SecureRandom();

    public LogServerManager(List<URI> serverUris) {
        this.serverUris = List.copyOf(serverUris);
    }

    Collection<URI> getServerUris(int replicaCount) {
        Preconditions.checkArgument(replicaCount > 0 && replicaCount <= this.serverUris.size(),
                "Too many or too few replicas (%s) requested; there are %s server(s) available.", replicaCount, this.serverUris.size());
        if (replicaCount == this.serverUris.size()) {
            return this.serverUris; // It's an immutable list, so we can just return it.
        }

        val result = new HashSet<URI>();
        while (result.size() < replicaCount) {
            result.add(this.serverUris.get(this.random.nextInt(this.serverUris.size())));
        }
        return result;
    }
}
