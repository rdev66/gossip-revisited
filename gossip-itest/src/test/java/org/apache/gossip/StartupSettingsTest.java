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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.gossip.manager.GossipManager;
import org.apache.gossip.manager.GossipManagerBuilder;
import org.junit.jupiter.api.Test;

/** Tests support of using {@code StartupSettings} and thereby reading setup config from file. */
@Slf4j
public class StartupSettingsTest {
  private static final String CLUSTER = UUID.randomUUID().toString();

  @Test
  public void testUsingSettingsFile() throws IOException, URISyntaxException {
    File settingsFile = File.createTempFile("gossipTest", ".json");
    settingsFile.deleteOnExit();
    writeSettingsFile(settingsFile);
    URI uri = new URI("udp://" + "127.0.0.1" + ":" + 50000);
    GossipSettings firstGossipSettings = new GossipSettings();
    firstGossipSettings.setTransportManagerClass(
        "org.apache.gossip.transport.UnitTestTransportManager");
    firstGossipSettings.setProtocolManagerClass(
        "org.apache.gossip.protocol.UnitTestProtocolManager");
    GossipManager firstService =
        GossipManagerBuilder.newBuilder()
            .cluster(CLUSTER)
            .uri(uri)
            .id("1")
            .gossipSettings(firstGossipSettings)
            .build();
    firstService.init();
    GossipManager manager =
        GossipManagerBuilder.newBuilder()
            .startupSettings(StartupSettings.fromJSONFile(settingsFile))
            .build();
    manager.init();
    firstService.shutdown();
    manager.shutdown();
  }

  private void writeSettingsFile(File target) throws IOException {
    String settings =
        "[{\n"
            + // It is odd that this is meant to be in an array, but oh well.
            "  \"cluster\":\""
            + CLUSTER
            + "\",\n"
            + "  \"id\":\""
            + "2"
            + "\",\n"
            + "  \"uri\":\"udp://127.0.0.1:50001\",\n"
            + "  \"gossip_interval\":1000,\n"
            + "  \"window_size\":1000,\n"
            + "  \"minimum_samples\":5,\n"
            + "  \"cleanup_interval\":10000,\n"
            + "  \"convict_threshold\":2.6,\n"
            + "  \"distribution\":\"exponential\",\n"
            + "  \"transport_manager_class\":\"org.apache.gossip.transport.UnitTestTransportManager\",\n"
            + "  \"protocol_manager_class\":\"org.apache.gossip.protocol.UnitTestProtocolManager\",\n"
            + "  \"properties\":{},\n"
            + "  \"members\":[\n"
            + "    {\"cluster\": \""
            + CLUSTER
            + "\",\"uri\":\"udp://127.0.0.1:5000\"}\n"
            + "  ]\n"
            + "}]";

    log.info("Using settings file with contents of:\n---\n" + settings + "\n---");
    FileOutputStream output = new FileOutputStream(target);
    output.write(settings.getBytes());
    output.close();
  }
}
