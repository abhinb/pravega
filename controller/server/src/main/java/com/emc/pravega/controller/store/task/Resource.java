/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.store.task;

import com.google.common.base.Preconditions;
import lombok.Data;

/**
 * Resources managed by controller
 * 1. Stream resource: scope/streamName
 * 2, Tx resource:     scope/streamName/txId
 */
@Data
public class Resource {
    private final String string;

    public Resource(String... parts) {
        Preconditions.checkNotNull(parts);
        Preconditions.checkArgument(parts.length > 0);
        String representation = parts[0];
        for (int i = 1; i < parts.length; i++) {
            representation += "/" + parts[i];
        }
        string = representation;
    }
}
