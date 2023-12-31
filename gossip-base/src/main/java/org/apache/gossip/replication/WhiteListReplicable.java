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
package org.apache.gossip.replication;

import java.util.ArrayList;
import java.util.List;
import org.apache.gossip.LocalMember;
import org.apache.gossip.model.Base;

/**
 * Replicable implementation which replicates data to given set of nodes.
 *
 * @param <T> A subtype of the class {@link org.apache.gossip.model.Base} which uses this interface
 * @see Replicable
 */
public class WhiteListReplicable<T extends Base> implements Replicable<T> {

  private final List<LocalMember> whiteListMembers;

  public WhiteListReplicable(List<LocalMember> whiteListMembers) {
    if (whiteListMembers == null) {
      this.whiteListMembers = new ArrayList<>();
    } else {
      this.whiteListMembers = whiteListMembers;
    }
  }

  public List<LocalMember> getWhiteListMembers() {
    return whiteListMembers;
  }

  @Override
  public boolean shouldReplicate(LocalMember me, LocalMember destination, T message) {
    return whiteListMembers.contains(destination);
  }
}
