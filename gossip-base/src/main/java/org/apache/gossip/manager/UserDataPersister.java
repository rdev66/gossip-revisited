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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.gossip.model.PerNodeDataMessage;
import org.apache.gossip.model.SharedDataMessage;

@Slf4j
public class UserDataPersister implements Runnable {

  private final GossipCore gossipCore;

  private final File perNodePath;
  private final File sharedPath;
  private final ObjectMapper objectMapper;

  UserDataPersister(GossipCore gossipCore, File perNodePath, File sharedPath) {
    this.gossipCore = gossipCore;
    this.objectMapper = GossipManager.metdataObjectMapper;
    this.perNodePath = perNodePath;
    this.sharedPath = sharedPath;
  }

  @SuppressWarnings("unchecked")
  ConcurrentHashMap<String, ConcurrentHashMap<String, PerNodeDataMessage>> readPerNodeFromDisk() {
    if (!perNodePath.exists()) {
      return new ConcurrentHashMap<>();
    }
    try (FileInputStream fos = new FileInputStream(perNodePath)) {
      return objectMapper.readValue(fos, ConcurrentHashMap.class);
    } catch (IOException e) {
      log.error("Error!", e);
    }
    return new ConcurrentHashMap<>();
  }

  void writePerNodeToDisk() {
    try (FileOutputStream fos = new FileOutputStream(perNodePath)) {
      objectMapper.writeValue(fos, gossipCore.getPerNodeData());
    } catch (IOException e) {
      log.error("Error!", e);
    }
  }

  void writeSharedToDisk() {
    try (FileOutputStream fos = new FileOutputStream(sharedPath)) {
      objectMapper.writeValue(fos, gossipCore.getSharedData());
    } catch (IOException e) {
      log.error("Error!", e);
    }
  }

  @SuppressWarnings("unchecked")
  ConcurrentHashMap<String, SharedDataMessage> readSharedDataFromDisk() {
    if (!sharedPath.exists()) {
      return new ConcurrentHashMap<>();
    }
    try (FileInputStream fos = new FileInputStream(sharedPath)) {
      return objectMapper.readValue(fos, ConcurrentHashMap.class);
    } catch (IOException e) {
      log.error("Error!", e);
    }
    return new ConcurrentHashMap<>();
  }

  /** Writes all pernode and shared data to disk */
  @Override
  public void run() {
    writePerNodeToDisk();
    writeSharedToDisk();
  }
}
