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
package org.apache.gossip.manager;

import static org.junit.jupiter.api.Assertions.*;

import com.codahale.metrics.MetricRegistry;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.gossip.GossipSettings;
import org.apache.gossip.LocalMember;
import org.apache.gossip.Member;
import org.apache.gossip.manager.handlers.MessageHandler;
import org.apache.gossip.manager.handlers.ResponseHandler;
import org.apache.gossip.manager.handlers.TypedMessageHandlerWrapper;
import org.apache.gossip.model.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GossipManagerBuilderTest {

  private GossipManagerBuilder.ManagerBuilder builder;

  @BeforeEach
  public void setup() throws Exception {
    builder =
        GossipManagerBuilder.newBuilder()
            .id("id")
            .cluster("aCluster")
            .uri(new URI("udp://localhost:2000"))
            .gossipSettings(new GossipSettings());
  }

  @Test
  public void idShouldNotBeNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          GossipManagerBuilder.newBuilder().cluster("aCluster").build();
        });
  }

  @Test
  public void clusterShouldNotBeNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          GossipManagerBuilder.newBuilder().id("id").build();
        });
  }

  @Test
  public void settingsShouldNotBeNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          GossipManagerBuilder.newBuilder().id("id").cluster("aCluster").build();
        });
  }

  @Test
  public void createMembersListIfNull() {
    GossipManager gossipManager =
        builder.gossipMembers(null).registry(new MetricRegistry()).build();
    assertNotNull(gossipManager.getLiveMembers());
  }

  @Test
  public void createDefaultMessageHandlerIfNull() {
    GossipManager gossipManager =
        builder.messageHandler(null).registry(new MetricRegistry()).build();
    assertNotNull(gossipManager.getMessageHandler());
  }

  @Test
  public void testMessageHandlerKeeping() {
    MessageHandler mi = new TypedMessageHandlerWrapper(Response.class, new ResponseHandler());
    GossipManager gossipManager = builder.messageHandler(mi).registry(new MetricRegistry()).build();
    assertNotNull(gossipManager.getMessageHandler());
    assertEquals(gossipManager.getMessageHandler(), mi);
  }

  @Test
  public void useMemberListIfProvided() throws URISyntaxException {
    LocalMember member =
        new LocalMember(
            "aCluster",
            new URI("udp://localhost:2000"),
            "aGossipMember",
            System.nanoTime(),
            new HashMap<String, String>(),
            1000,
            1,
            "exponential");
    List<Member> memberList = new ArrayList<>();
    memberList.add(member);
    GossipManager gossipManager =
        builder
            .uri(new URI("udp://localhost:8000"))
            .gossipMembers(memberList)
            .registry(new MetricRegistry())
            .build();
    assertEquals(1, gossipManager.getDeadMembers().size());
    assertEquals(member.getId(), gossipManager.getDeadMembers().get(0).getId());
  }
}
