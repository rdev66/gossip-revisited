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
package org.apache.gossip;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MemberTest {

  @Test
  public void testHashCodeFromGossip40() throws URISyntaxException {
    Assertions.assertNotEquals(new LocalMember(
            "mycluster",
            new URI("udp://4.4.4.4:1000"),
            "myid",
            1,
            new HashMap<>(),
            10,
            5,
            "exponential")
        .hashCode(), new LocalMember(
                "mycluster",
                new URI("udp://4.4.4.5:1005"),
                "yourid",
                11,
                new HashMap<>(),
                11,
                6,
                "exponential")
            .hashCode());
  }
}
