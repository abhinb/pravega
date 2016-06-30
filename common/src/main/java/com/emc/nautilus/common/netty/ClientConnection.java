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
package com.emc.nautilus.common.netty;

public interface ClientConnection {

    /**
     * Sends the provided command. This operation may block. (Though buffering is used to try to
     * prevent it)
     * 
     * @throws ConnectionFailedException The connection has died, and can no longer be used.
     */
    void send(WireCommand cmd); // TODO: Should throw ConnectionFailedException

    /**
     * @param cp Sets the command processor to receive incoming replies from the server. This method
     *            may only be called once.
     */
    void setResponseProcessor(ReplyProcessor cp);

    /**
     * Drop the connection. No further operations may be performed.
     */
    void drop();

    boolean isConnected();

}
