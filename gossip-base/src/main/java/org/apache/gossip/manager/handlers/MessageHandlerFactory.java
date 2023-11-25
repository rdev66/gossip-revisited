/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gossip.manager.handlers;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.gossip.model.*;

public class MessageHandlerFactory {

  public static MessageHandler defaultHandler() {
    return concurrentHandler(
        new TypedMessageHandlerWrapper(Response.class, new ResponseHandler()),
        new TypedMessageHandlerWrapper(ShutdownMessage.class, new ShutdownMessageHandler()),
        new TypedMessageHandlerWrapper(PerNodeDataMessage.class, new PerNodeDataMessageHandler()),
        new TypedMessageHandlerWrapper(SharedDataMessage.class, new SharedDataMessageHandler()),
        new TypedMessageHandlerWrapper(ActiveGossipMessage.class, new ActiveGossipMessageHandler()),
        new TypedMessageHandlerWrapper(
            PerNodeDataBulkMessage.class, new PerNodeDataBulkMessageHandler()),
        new TypedMessageHandlerWrapper(
            SharedDataBulkMessage.class, new SharedDataBulkMessageHandler()));
  }

  public static MessageHandler concurrentHandler(MessageHandler... handlers) {
    if (handlers == null) throw new NullPointerException("handlers cannot be null");
    if (Arrays.stream(handlers).filter(Objects::nonNull).count() != handlers.length) {
      throw new NullPointerException("found at least one null handler");
    }
    return (gossipCore, gossipManager, base) -> {
      // return true if at least one of the component handlers return true.
      return Stream.of(handlers).filter((mi) -> mi.invoke(gossipCore, gossipManager, base)).count()
          > 0;
    };
  }
}
