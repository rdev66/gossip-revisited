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

import org.apache.gossip.manager.GossipCore;
import org.apache.gossip.manager.GossipManager;
import org.apache.gossip.model.ActiveGossipMessage;
import org.apache.gossip.model.Base;
import org.apache.gossip.udp.UdpSharedDataMessage;
import org.junit.Assert;
import org.junit.Test;

public class MessageHandlerTest {
  @Test
  public void testSimpleHandler() {
    MessageHandler mi =
        new TypedMessageHandlerWrapper(FakeMessage.class, new DummyMessageHandler());
    Assert.assertTrue(mi.invoke(null, null, new FakeMessage()));
    Assert.assertFalse(mi.invoke(null, null, new ActiveGossipMessage()));
  }

  @Test(expected = NullPointerException.class)
  public void testSimpleHandlerNullClassConstructor() {
    new TypedMessageHandlerWrapper(null, new DummyMessageHandler());
  }

  @Test(expected = NullPointerException.class)
  public void testSimpleHandlerNullHandlerConstructor() {
    new TypedMessageHandlerWrapper(FakeMessage.class, null);
  }

  @Test
  public void testCallCountSimpleHandler() {
    DummyMessageHandler h = new DummyMessageHandler();
    MessageHandler mi = new TypedMessageHandlerWrapper(FakeMessage.class, h);
    mi.invoke(null, null, new FakeMessage());
    Assert.assertEquals(1, h.counter);
    mi.invoke(null, null, new ActiveGossipMessage());
    Assert.assertEquals(1, h.counter);
    mi.invoke(null, null, new FakeMessage());
    Assert.assertEquals(2, h.counter);
  }

  @Test(expected = NullPointerException.class)
  @SuppressWarnings("all")
  public void cantAddNullHandler() {
    MessageHandler handler = MessageHandlerFactory.concurrentHandler();
  }

  @Test(expected = NullPointerException.class)
  public void cantAddNullHandler2() {
    MessageHandlerFactory.concurrentHandler(
        new TypedMessageHandlerWrapper(FakeMessage.class, new DummyMessageHandler()),
        null,
        new TypedMessageHandlerWrapper(FakeMessage.class, new DummyMessageHandler()));
  }

  @Test
  public void testMessageHandlerCombiner() {
    // Empty combiner - false result
    MessageHandler mi =
        MessageHandlerFactory.concurrentHandler((gossipCore, gossipManager, base) -> false);
    Assert.assertFalse(mi.invoke(null, null, new Base()));

    DummyMessageHandler h = new DummyMessageHandler();
    mi =
        MessageHandlerFactory.concurrentHandler(
            new TypedMessageHandlerWrapper(FakeMessage.class, h),
            new TypedMessageHandlerWrapper(FakeMessage.class, h));

    Assert.assertTrue(mi.invoke(null, null, new FakeMessage()));
    Assert.assertFalse(mi.invoke(null, null, new ActiveGossipMessage()));
    Assert.assertEquals(2, h.counter);

    // Increase size in runtime. Should be 3 calls: 2+3 = 5
    mi =
        MessageHandlerFactory.concurrentHandler(
            mi, new TypedMessageHandlerWrapper(FakeMessage.class, h));
    Assert.assertTrue(mi.invoke(null, null, new FakeMessage()));
    Assert.assertEquals(5, h.counter);
  }

  @Test
  public void testMessageHandlerCombiner2levels() {
    DummyMessageHandler messageHandler = new DummyMessageHandler();

    MessageHandler mi1 =
        MessageHandlerFactory.concurrentHandler(
            new TypedMessageHandlerWrapper(FakeMessage.class, messageHandler),
            new TypedMessageHandlerWrapper(FakeMessage.class, messageHandler));

    MessageHandler mi2 =
        MessageHandlerFactory.concurrentHandler(
            new TypedMessageHandlerWrapper(FakeMessage.class, messageHandler),
            new TypedMessageHandlerWrapper(FakeMessage.class, messageHandler));

    MessageHandler mi = MessageHandlerFactory.concurrentHandler(mi1, mi2);

    Assert.assertTrue(mi.invoke(null, null, new FakeMessage()));
    Assert.assertEquals(4, messageHandler.counter);
  }

  @Test
  public void testMessageHandlerCombinerDataShipping() {
    MessageHandler mi = MessageHandlerFactory.concurrentHandler();
    FakeMessageDataHandler h = new FakeMessageDataHandler();
    mi =
        MessageHandlerFactory.concurrentHandler(
            mi, new TypedMessageHandlerWrapper(FakeMessageData.class, h));

    Assert.assertTrue(mi.invoke(null, null, new FakeMessageData(101)));
    Assert.assertEquals(101, h.data);
  }

  @Test
  public void testCombiningDefaultHandler() {
    MessageHandler mi =
        MessageHandlerFactory.concurrentHandler(
            MessageHandlerFactory.defaultHandler(),
            new TypedMessageHandlerWrapper(FakeMessage.class, new DummyMessageHandler()));
    // UdpSharedGossipDataMessage with null gossipCore -> exception
    boolean thrown = false;
    try {
      mi.invoke(null, null, new UdpSharedDataMessage());
    } catch (NullPointerException e) {
      thrown = true;
    }
    Assert.assertTrue(thrown);
    // skips FakeMessage and FakeHandler works ok
    Assert.assertTrue(mi.invoke(null, null, new FakeMessage()));
  }

  private static class FakeMessage extends Base {
    public FakeMessage() {}
  }

  private static class FakeMessageData extends Base {
    public int data;

    public FakeMessageData(int data) {
      this.data = data;
    }
  }

  private static class FakeMessageDataHandler implements MessageHandler {
    public int data;

    public FakeMessageDataHandler() {
      data = 0;
    }

    public boolean invoke(GossipCore gossipCore, GossipManager gossipManager, Base base) {
      data = ((FakeMessageData) base).data;
      return true;
    }
  }

  private static class DummyMessageHandler implements MessageHandler {
    public int counter;

    public DummyMessageHandler() {
      counter = 0;
    }

    public boolean invoke(GossipCore gossipCore, GossipManager gossipManager, Base base) {
      counter++;
      return true;
    }
  }
}
