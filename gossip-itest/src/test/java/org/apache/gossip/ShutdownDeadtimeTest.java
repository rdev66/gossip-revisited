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

import io.teknek.tunit.TUnit;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.gossip.manager.GossipManager;
import org.apache.gossip.manager.GossipManagerBuilder;
import org.junit.jupiter.api.Test;

@Slf4j
public class ShutdownDeadtimeTest {

  // Note: this test is floppy depending on the values in GossipSettings (smaller values seem to do
  // harm), and the
  //       sleep that happens after startup.
  @Test
  public void DeadNodesDoNotComeAliveAgain() throws InterruptedException, URISyntaxException {
    GossipSettings settings = new GossipSettings(100, 10000, 1000, 1, 10.0, "normal", false);
    settings.setPersistRingState(false);
    settings.setPersistDataState(false);
    String cluster = UUID.randomUUID().toString();
    int seedNodes = 3;
    List<Member> startupMembers = new ArrayList<>();
    for (int i = 1; i < seedNodes + 1; ++i) {
      URI uri = new URI("udp://" + "127.0.0.1" + ":" + (30300 + i));
      startupMembers.add(new RemoteMember(cluster, uri, i + ""));
    }
    final List<GossipManager> clients =
        Collections.synchronizedList(new ArrayList<GossipManager>());
    final int clusterMembers = 5;
    for (int i = 1; i < clusterMembers + 1; ++i) {
      URI uri = new URI("udp://" + "127.0.0.1" + ":" + (30300 + i));
      GossipManager gossipService =
          GossipManagerBuilder.newBuilder()
              .cluster(cluster)
              .uri(uri)
              .id(i + "")
              .gossipMembers(startupMembers)
              .gossipSettings(settings)
              .build();
      clients.add(gossipService);
      gossipService.init();
      Thread.sleep(1000);
    }
    TUnit.assertThat(
            () -> {
              int total = 0;
              for (int i = 0; i < clusterMembers; ++i) {
                total += clients.get(i).getLiveMembers().size();
              }
              return total;
            })
        .afterWaitingAtMost(40, TimeUnit.SECONDS)
        .isEqualTo(20);

    // shutdown one client and verify that one client is lost.
    Random r = new Random();
    int randomClientId = r.nextInt(clusterMembers);
    log.info("shutting down " + randomClientId);
    final int shutdownPort = clients.get(randomClientId).getMyself().getUri().getPort();
    final String shutdownId = clients.get(randomClientId).getMyself().getId();
    clients.get(randomClientId).shutdown();
    TUnit.assertThat(
            () -> {
              int total = 0;
              for (int i = 0; i < clusterMembers; ++i) {
                total += clients.get(i).getLiveMembers().size();
              }
              return total;
            })
        .afterWaitingAtMost(40, TimeUnit.SECONDS)
        .isEqualTo(16);
    clients.remove(randomClientId);

    TUnit.assertThat(
            () -> {
              int total = 0;
              for (int i = 0; i < clusterMembers - 1; ++i) {
                total += clients.get(i).getDeadMembers().size();
              }
              return total;
            })
        .afterWaitingAtMost(50, TimeUnit.SECONDS)
        .isEqualTo(4);

    URI uri = new URI("udp://" + "127.0.0.1" + ":" + shutdownPort);
    // start client again
    GossipManager gossipService =
        GossipManagerBuilder.newBuilder()
            .gossipSettings(settings)
            .cluster(cluster)
            .uri(uri)
            .id(shutdownId)
            .gossipMembers(startupMembers)
            .build();
    clients.add(gossipService);
    gossipService.init();

    // verify that the client is alive again for every node
    TUnit.assertThat(
            () -> {
              int total = 0;
              for (int i = 0; i < clusterMembers; ++i) {
                total += clients.get(i).getLiveMembers().size();
              }
              return total;
            })
        .afterWaitingAtMost(60, TimeUnit.SECONDS)
        .isEqualTo(20);

    for (int i = 0; i < clusterMembers; ++i) {
      final int j = i;
      new Thread(() -> clients.get(j).shutdown()).start();
    }
  }
}
