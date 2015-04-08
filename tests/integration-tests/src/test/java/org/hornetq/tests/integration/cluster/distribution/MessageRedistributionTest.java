/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.integration.cluster.distribution;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.server.Bindable;
import org.hornetq.core.server.cluster.impl.Redistributor;
import org.hornetq.core.server.group.impl.GroupingHandlerConfiguration;
import org.hornetq.core.server.impl.QueueImpl;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.junit.Before;
import org.junit.Test;

/**
 * A MessageRedistributionTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *         <p/>
 *         Created 10 Feb 2009 18:41:57
 */
public class MessageRedistributionTest extends ClusterTestBase
{
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      start();
   }

   private void start() throws Exception
   {
      setupServers();

      setRedistributionDelay(0);
   }

   protected boolean isNetty()
   {
      return false;
   }

   //https://issues.jboss.org/browse/HORNETQ-1061
   @Test
   public void testRedistributionWithMessageGroups() throws Exception
   {
      setupCluster(false);

      MessageRedistributionTest.log.info("Doing test");


      getServer(0).getConfiguration().setGroupingHandlerConfiguration(
         new GroupingHandlerConfiguration(new SimpleString("handler"), GroupingHandlerConfiguration.TYPE.LOCAL, new SimpleString("queues")));
      getServer(1).getConfiguration().setGroupingHandlerConfiguration(
         new GroupingHandlerConfiguration(new SimpleString("handler"), GroupingHandlerConfiguration.TYPE.REMOTE, new SimpleString("queues")));

      startServers(0, 1);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());

      this.

         createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);

      addConsumer(1, 1, "queue0", null);
      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 1, 1, false);
      waitForBindings(1, "queues.testaddress", 1, 0, false);

      //send some grouped messages before we add the consumer to node 0 so we guarantee its pinned to node 1
      sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("grp1"));
      addConsumer(0, 0, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 1, 1, false);
      waitForBindings(1, "queues.testaddress", 1, 1, false);
      //now send some non grouped messages
      send(0, "queues.testaddress", 10, false, null);

      //consume half of the grouped messages from node 1
      for (int i = 0; i < 5; i++)
      {
         ClientMessage message = getConsumer(1).receive(1000);
         assertNotNull(message);
         message.acknowledge();
         assertNotNull(message.getSimpleStringProperty(Message.HDR_GROUP_ID));
      }

      //now consume the non grouped messages from node 1 where they are pinned
      for (int i = 0; i < 5; i++)
      {
         ClientMessage message = getConsumer(0).receive(5000);
         assertNotNull("" + i, message);
         message.acknowledge();
         assertNull(message.getSimpleStringProperty(Message.HDR_GROUP_ID));
      }

      ClientMessage clientMessage = getConsumer(0).receiveImmediate();
      assertNull(clientMessage);

      // i know the last 5 messages consumed won't be acked yet so i wait for 15
      waitForMessages(1, "queues.testaddress", 15);

      //now removing it will start redistribution but only for non grouped messages
      removeConsumer(1);

      //consume the non grouped messages
      for (int i = 0; i < 5; i++)
      {
         ClientMessage message = getConsumer(0).receive(5000);
         if (message == null)
         {
            System.out.println();
         }
         assertNotNull("" + i, message);
         message.acknowledge();
         assertNull(message.getSimpleStringProperty(Message.HDR_GROUP_ID));
      }

      clientMessage = getConsumer(0).receiveImmediate();
      assertNull(clientMessage);

      removeConsumer(0);

      addConsumer(1, 1, "queue0", null);

      //now we see the grouped messages are still on the same node
      for (int i = 0; i < 5; i++)
      {
         ClientMessage message = getConsumer(1).receive(1000);
         assertNotNull(message);
         message.acknowledge();
         assertNotNull(message.getSimpleStringProperty(Message.HDR_GROUP_ID));
      }
      MessageRedistributionTest.log.info("Test done");
   }

   //https://issues.jboss.org/browse/HORNETQ-1057
   @Test
   public void testRedistributionStopsWhenConsumerAdded() throws Exception
   {
      setupCluster(false);

      MessageRedistributionTest.log.info("Doing test");

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 0, false);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      send(0, "queues.testaddress", 2000, false, null);

      removeConsumer(0);
      addConsumer(0, 0, "queue0", null);

      Bindable bindable = servers[0].getPostOffice().getBinding(new SimpleString("queue0")).getBindable();
      String debug = ((QueueImpl)bindable).debug();
      assertFalse(debug.contains(Redistributor.class.getName()));
      MessageRedistributionTest.log.info("Test done");
   }

   @Test
   public void testRedistributionWhenConsumerIsClosed() throws Exception
   {
      setupCluster(false);

      MessageRedistributionTest.log.info("Doing test");

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      send(0, "queues.testaddress", 20, false, null);

      getReceivedOrder(0);
      int[] ids1 = getReceivedOrder(1);
      getReceivedOrder(2);

      removeConsumer(1);

      verifyReceiveRoundRobinInSomeOrderWithCounts(false, ids1, 0, 2);

      MessageRedistributionTest.log.info("Test done");
   }

   @Test
   public void testRedistributionWhenConsumerIsClosedNotConsumersOnAllNodes() throws Exception
   {
      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      send(0, "queues.testaddress", 20, false, null);

      int[] ids1 = getReceivedOrder(1);
      getReceivedOrder(2);

      removeConsumer(1);

      verifyReceiveRoundRobinInSomeOrderWithCounts(false, ids1, 2);
   }

   @Test
   public void testNoRedistributionWhenConsumerIsClosedForwardWhenNoConsumersTrue() throws Exception
   {
      // x
      setupCluster(true);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      send(0, "queues.testaddress", 20, false, null);

      removeConsumer(1);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      addConsumer(1, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      verifyReceiveRoundRobinInSomeOrder(20, 0, 1, 2);
   }

   @Test
   public void testNoRedistributionWhenConsumerIsClosedNoConsumersOnOtherNodes() throws Exception
   {
      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(1, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      send(0, "queues.testaddress", 20, false, null);

      removeConsumer(1);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 0, false);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 2, 0, false);

      addConsumer(1, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      verifyReceiveAll(20, 1);
   }

   @Test
   public void testRedistributeWithScheduling() throws Exception
   {
      setupCluster(false);

      AddressSettings setting = new AddressSettings();
      setting.setRedeliveryDelay(10000);
      servers[0].getAddressSettingsRepository().addMatch("queues.testaddress", setting);
      servers[0].getAddressSettingsRepository().addMatch("queue0", setting);
      servers[1].getAddressSettingsRepository().addMatch("queue0", setting);
      servers[1].getAddressSettingsRepository().addMatch("queues.testaddress", setting);

      startServers(0);

      setupSessionFactory(0, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);

      ClientSession session0 = sfs[0].createSession(false, false, false);

      ClientProducer prod0 = session0.createProducer("queues.testaddress");

      for (int i = 0; i < 100; i++)
      {
         ClientMessage msg = session0.createMessage(true);
         msg.putIntProperty("key", i);

         byte[] bytes = new byte[24];

         ByteBuffer bb = ByteBuffer.wrap(bytes);

         bb.putLong(i);

         msg.putBytesProperty(MessageImpl.HDR_BRIDGE_DUPLICATE_ID, bytes);

         prod0.send(msg);

         session0.commit();
      }

      session0.close();

      session0 = sfs[0].createSession(true, false, false);

      ClientConsumer consumer0 = session0.createConsumer("queue0");

      session0.start();

      ArrayList<Xid> xids = new ArrayList<Xid>();

      for (int i = 0; i < 100; i++)
      {
         Xid xid = newXID();

         session0.start(xid, XAResource.TMNOFLAGS);

         ClientMessage msg = consumer0.receive(5000);

         msg.acknowledge();

         session0.end(xid, XAResource.TMSUCCESS);

         session0.prepare(xid);

         xids.add(xid);
      }

      session0.close();

      sfs[0].close();
      sfs[0] = null;

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      ClientSession session1 = sfs[1].createSession(false, false);
      session1.start();
      ClientConsumer consumer1 = session1.createConsumer("queue0");

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      session0 = sfs[0].createSession(true, false, false);

      for (Xid xid : xids)
      {
         session0.rollback(xid);
      }

      for (int i = 0; i < 100; i++)
      {
         ClientMessage msg = consumer1.receive(15000);
         assertNotNull(msg);
         msg.acknowledge();
      }

      session1.commit();

   }

   @Test
   public void testRedistributionWhenConsumerIsClosedQueuesWithFilters() throws Exception
   {
      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      String filter1 = "giraffe";
      String filter2 = "platypus";

      createQueue(0, "queues.testaddress", "queue0", filter1, false);
      createQueue(1, "queues.testaddress", "queue0", filter2, false);
      createQueue(2, "queues.testaddress", "queue0", filter1, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      send(0, "queues.testaddress", 20, false, filter1);

      int[] ids0 = getReceivedOrder(0);
      getReceivedOrder(1);
      getReceivedOrder(2);

      removeConsumer(0);

      verifyReceiveRoundRobinInSomeOrderWithCounts(false, ids0, 2);
   }

   @Test
   public void testRedistributionWhenConsumerIsClosedConsumersWithFilters() throws Exception
   {
      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      String filter1 = "giraffe";
      String filter2 = "platypus";

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", filter1);
      addConsumer(1, 1, "queue0", filter2);
      addConsumer(2, 2, "queue0", filter1);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      send(0, "queues.testaddress", 20, false, filter1);

      int[] ids0 = getReceivedOrder(0);
      getReceivedOrder(1);
      getReceivedOrder(2);

      removeConsumer(0);

      verifyReceiveRoundRobinInSomeOrderWithCounts(false, ids0, 2);
   }

   @Test
   public void testRedistributionWhenRemoteConsumerIsAdded() throws Exception
   {
      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 0, false);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      send(0, "queues.testaddress", 20, false, null);

      removeConsumer(0);

      addConsumer(1, 1, "queue0", null);

      verifyReceiveAll(20, 1);
      verifyNotReceive(1);
   }

   @Test
   public void testBackAndForth() throws Exception
   {
      for (int i = 0; i < 10; i++)
      {
         setupCluster(false);

         startServers(0, 1, 2);

         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());
         setupSessionFactory(2, isNetty());

         final String ADDRESS = "queues.testaddress";
         final String QUEUE = "queue0";

         createQueue(0, ADDRESS, QUEUE, null, false);
         createQueue(1, ADDRESS, QUEUE, null, false);
         createQueue(2, ADDRESS, QUEUE, null, false);

         addConsumer(0, 0, QUEUE, null);

         waitForBindings(0, ADDRESS, 1, 1, true);
         waitForBindings(1, ADDRESS, 1, 0, true);
         waitForBindings(2, ADDRESS, 1, 0, true);

         waitForBindings(0, ADDRESS, 2, 0, false);
         waitForBindings(1, ADDRESS, 2, 1, false);
         waitForBindings(2, ADDRESS, 2, 1, false);

         send(0, ADDRESS, 20, false, null);

         waitForMessages(0, ADDRESS, 20);

         removeConsumer(0);

         waitForBindings(0, ADDRESS, 1, 0, true);
         waitForBindings(1, ADDRESS, 1, 0, true);
         waitForBindings(2, ADDRESS, 1, 0, true);

         waitForBindings(0, ADDRESS, 2, 0, false);
         waitForBindings(1, ADDRESS, 2, 0, false);
         waitForBindings(2, ADDRESS, 2, 0, false);

         addConsumer(1, 1, QUEUE, null);

         waitForBindings(0, ADDRESS, 1, 0, true);
         waitForBindings(1, ADDRESS, 1, 1, true);
         waitForBindings(2, ADDRESS, 1, 0, true);

         waitForMessages(1, ADDRESS, 20);
         waitForMessages(0, ADDRESS, 0);

         waitForBindings(0, ADDRESS, 2, 1, false);
         waitForBindings(1, ADDRESS, 2, 0, false);
         waitForBindings(2, ADDRESS, 2, 1, false);

         removeConsumer(1);

         waitForBindings(0, ADDRESS, 1, 0, true);
         waitForBindings(1, ADDRESS, 1, 0, true);
         waitForBindings(2, ADDRESS, 1, 0, true);

         waitForBindings(0, ADDRESS, 2, 0, false);
         waitForBindings(1, ADDRESS, 2, 0, false);
         waitForBindings(2, ADDRESS, 2, 0, false);

         addConsumer(0, 0, QUEUE, null);

         waitForBindings(0, ADDRESS, 1, 1, true);
         waitForBindings(1, ADDRESS, 1, 0, true);
         waitForBindings(2, ADDRESS, 1, 0, true);

         waitForBindings(0, ADDRESS, 2, 0, false);
         waitForBindings(1, ADDRESS, 2, 1, false);
         waitForBindings(2, ADDRESS, 2, 1, false);

         waitForMessages(0, ADDRESS, 20);

         verifyReceiveAll(20, 0);
         verifyNotReceive(0);

         addConsumer(1, 1, QUEUE, null);
         verifyNotReceive(1);
         removeConsumer(1);

         stopServers();
         start();
      }

   }

   // https://issues.jboss.org/browse/HORNETQ-1072
   @Test
   public void testBackAndForth2WithDuplicDetection() throws Exception
   {
      internalTestBackAndForth2(true);
   }

   @Test
   public void testBackAndForth2() throws Exception
   {
      internalTestBackAndForth2(false);
   }

   public void internalTestBackAndForth2(final boolean useDuplicateDetection) throws Exception
   {
      AtomicInteger duplDetection = null;

      if (useDuplicateDetection)
      {
         duplDetection = new AtomicInteger(0);
      }
      for (int i = 0; i < 10; i++)
      {
         setupCluster(false);

         startServers(0, 1);

         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());

         final String ADDRESS = "queues.testaddress";
         final String QUEUE = "queue0";

         createQueue(0, ADDRESS, QUEUE, null, false);
         createQueue(1, ADDRESS, QUEUE, null, false);

         addConsumer(0, 0, QUEUE, null);

         waitForBindings(0, ADDRESS, 1, 1, true);
         waitForBindings(1, ADDRESS, 1, 0, true);

         waitForBindings(0, ADDRESS, 1, 0, false);
         waitForBindings(1, ADDRESS, 1, 1, false);

         send(1, ADDRESS, 20, false, null, duplDetection);

         waitForMessages(0, ADDRESS, 20);

         removeConsumer(0);

         waitForBindings(0, ADDRESS, 1, 0, true);
         waitForBindings(1, ADDRESS, 1, 0, true);

         waitForBindings(0, ADDRESS, 1, 0, false);
         waitForBindings(1, ADDRESS, 1, 0, false);

         addConsumer(1, 1, QUEUE, null);

         waitForMessages(1, ADDRESS, 20);
         waitForMessages(0, ADDRESS, 0);

         waitForBindings(0, ADDRESS, 1, 1, false);
         waitForBindings(1, ADDRESS, 1, 0, false);

         removeConsumer(1);

         addConsumer(0, 0, QUEUE, null);

         waitForMessages(1, ADDRESS, 0);
         waitForMessages(0, ADDRESS, 20);

         removeConsumer(0);
         addConsumer(1, 1, QUEUE, null);

         waitForMessages(1, ADDRESS, 20);
         waitForMessages(0, ADDRESS, 0);

         verifyReceiveAll(20, 1);

         stopServers();
         start();
      }

   }

   @Test
   public void testRedistributionToQueuesWhereNotAllMessagesMatch() throws Exception
   {
      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      String filter1 = "giraffe";
      String filter2 = "platypus";

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 0, false);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      sendInRange(0, "queues.testaddress", 0, 10, false, filter1);
      sendInRange(0, "queues.testaddress", 10, 20, false, filter2);

      removeConsumer(0);
      addConsumer(1, 1, "queue0", filter1);
      addConsumer(2, 2, "queue0", filter2);

      verifyReceiveAllInRange(0, 10, 1);
      verifyReceiveAllInRange(10, 20, 2);
   }

   @Test
   public void testDelayedRedistribution() throws Exception
   {
      final long delay = 1000;
      setRedistributionDelay(delay);

      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 0, false);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      send(0, "queues.testaddress", 20, false, null);

      long start = System.currentTimeMillis();

      removeConsumer(0);
      addConsumer(1, 1, "queue0", null);

      long minReceiveTime = start + delay;

      verifyReceiveAllNotBefore(minReceiveTime, 20, 1);
   }

   @Test
   public void testDelayedRedistributionCancelled() throws Exception
   {
      final long delay = 1000;
      setRedistributionDelay(delay);

      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 0, false);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      send(0, "queues.testaddress", 20, false, null);

      removeConsumer(0);
      addConsumer(1, 1, "queue0", null);

      Thread.sleep(delay / 2);

      // Add it back on the local queue - this should stop any redistributionm
      addConsumer(0, 0, "queue0", null);

      Thread.sleep(delay);

      verifyReceiveAll(20, 0);
   }

   @Test
   public void testRedistributionNumberOfMessagesGreaterThanBatchSize() throws Exception
   {
      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 0, false);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      send(0, "queues.testaddress", QueueImpl.REDISTRIBUTOR_BATCH_SIZE * 2, false, null);

      removeConsumer(0);
      addConsumer(1, 1, "queue0", null);

      verifyReceiveAll(QueueImpl.REDISTRIBUTOR_BATCH_SIZE * 2, 1);
   }

   /*
    * Start one node with no consumers and send some messages
    * Start another node add a consumer and verify all messages are redistribute
    * https://jira.jboss.org/jira/browse/HORNETQ-359
    */
   @Test
   public void testRedistributionWhenNewNodeIsAddedWithConsumer() throws Exception
   {
      setupCluster(false);

      startServers(0);

      setupSessionFactory(0, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);

      waitForBindings(0, "queues.testaddress", 1, 0, true);

      send(0, "queues.testaddress", 20, false, null);

      // Now bring up node 1

      startServers(1);

      setupSessionFactory(1, isNetty());

      createQueue(1, "queues.testaddress", "queue0", null, false);

      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(0, "queues.testaddress", 1, 0, false);

      addConsumer(0, 1, "queue0", null);

      verifyReceiveAll(20, 0);
      verifyNotReceive(0);
   }

   @Test
   public void testRedistributionWithPagingOnTarget() throws Exception
   {
      setupCluster(false);

      AddressSettings as = new AddressSettings();
      as.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      as.setPageSizeBytes(10000);
      as.setMaxSizeBytes(20000);

      getServer(0).getAddressSettingsRepository().addMatch("queues.*", as);
      getServer(1).getAddressSettingsRepository().addMatch("queues.*", as);
      getServer(2).getAddressSettingsRepository().addMatch("queues.*", as);

      startServers(0);

      startServers(1);

      waitForTopology(getServer(0), 2);
      waitForTopology(getServer(1), 2);

      setupSessionFactory(0, isNetty());

      setupSessionFactory(1, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, true);

      createQueue(1, "queues.testaddress", "queue0", null, true);

      waitForBindings(1, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 1, 0, false);

      getServer(0).getPagingManager().getPageStore(new SimpleString("queues.testaddress")).startPaging();

      ClientSession session0 = sfs[0].createSession(true, true, 0);
      ClientProducer producer0 = session0.createProducer("queues.testaddress");

      ClientConsumer consumer0 = session0.createConsumer("queue0");
      session0.start();


      ClientSession session1 = sfs[1].createSession(true, true, 0);
      ClientConsumer consumer1 = session1.createConsumer("queue0");
      session1.start();


      for (int i = 0; i < 10; i++)
      {
         ClientMessage msg = session0.createMessage(true);
         msg.putIntProperty("i", i);
         // send two identical messages so they are routed on the cluster
         producer0.send(msg);
         producer0.send(msg);

         msg = consumer0.receive(5000);
         assertNotNull(msg);
         assertEquals(i, msg.getIntProperty("i").intValue());
         // msg.acknowledge(); // -- do not ack message on consumer0, to make sure the messages will be paged

         msg = consumer1.receive(5000);
         assertNotNull(msg);
         assertEquals(i, msg.getIntProperty("i").intValue());
         msg.acknowledge();
      }

      session0.close();
      session1.close();
   }

   @Test
   public void testStarvationOnOneOfTwoNodes() throws Exception
   {
      setupCluster(false);

      startServers(0, 1);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());

      createStarvationAwareQueue(0, "queues.testaddress", "queue0", null, true);
      createStarvationAwareQueue(1, "queues.testaddress", "queue0", null, true);

      waitForBindings(0, "queues.testaddress", 1, 0, false);
      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 0, false);
      waitForBindings(1, "queues.testaddress", 1, 0, true);

      ClientSession session0 = addClientSession(sfs[0].createSession(true, true, 0));
      ClientSession session1 = addClientSession(sfs[1].createSession(true, true, 0));

      int num = 80;
      final CountDownLatch latch = new CountDownLatch(num);
      //consumer0 : 1 msg/sec
      FixedRateConsumer slowConsumer = createFixedRateConsumer(session0, "queue0", 1000, latch);
      //consumer1 : 20 msg/sec
      FixedRateConsumer fastConsumer = createFixedRateConsumer(session1, "queue0", 50L, latch);

      waitForBindings(0, "queues.testaddress", 1, 1, false);
      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, false);
      waitForBindings(1, "queues.testaddress", 1, 1, true);

      //sending 80 messages
      this.send(0, "queues.testaddress", num, false, null);

      boolean result = latch.await(30, TimeUnit.SECONDS);

      System.out.println("slow consumer got: " + slowConsumer.getMessageCount());
      System.out.println("fast consumer got: " + fastConsumer.getMessageCount());
      assertTrue(num + " messages should be all received within 10 sec, but: " + latch.getCount(), result);

      assertTrue(fastConsumer.getMessageCount() > slowConsumer.getMessageCount());
      session0.commit();
      session1.commit();
      //make sure queue is empty
      ClientSession.QueueQuery query0 = session0.queueQuery(new SimpleString("queue0"));
      ClientSession.QueueQuery query1 = session1.queueQuery(new SimpleString("queue0"));
      assertEquals(query0.getMessageCount(), 0);
      assertEquals(query1.getMessageCount(), 0);

      session0.close();
      session1.close();

      MessageRedistributionTest.log.info("Test done");
   }

   @Test
   public void testStarvationOnOneOfTwoNodesLazyNodeStart() throws Exception
   {
      setupCluster(false);

      startServers(0, 1);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());

      createStarvationAwareQueue(0, "queues.testaddress", "queue0", null, true);
      createStarvationAwareQueue(1, "queues.testaddress", "queue0", null, true);

      waitForBindings(0, "queues.testaddress", 1, 0, false);
      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 0, false);
      waitForBindings(1, "queues.testaddress", 1, 0, true);

      ClientSession session0 = addClientSession(sfs[0].createSession(true, true, 0));

      int num = 80;
      final CountDownLatch latch = new CountDownLatch(num);
      //consumer0 : 1 msg/sec
      FixedRateConsumer slowConsumer = createFixedRateConsumer(session0, "queue0", 1000, latch);

      waitForBindings(0, "queues.testaddress", 1, 0, false);
      waitForBindings(0, "queues.testaddress", 1, 1, true);

      sfs[1].close();
      sfs[1] = null;
      servers[1].stop();

      //sending 80 messages
      this.send(0, "queues.testaddress", num, false, null);

      Thread.sleep(2000L);

      startServers(1);

      waitForBindings(0, "queues.testaddress", 1, 0, false);
      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, false);
      waitForBindings(1, "queues.testaddress", 1, 0, true);

      setupSessionFactory(1, isNetty());
      ClientSession session1 = addClientSession(sfs[1].createSession(true, true, 0));
      //consumer1 : 20 msg/sec
      FixedRateConsumer fastConsumer = createFixedRateConsumer(session1, "queue0", 50L, latch);

      waitForBindings(0, "queues.testaddress", 1, 1, false);
      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, false);
      waitForBindings(1, "queues.testaddress", 1, 1, true);

      boolean result = latch.await(30, TimeUnit.SECONDS);

      System.out.println("slow consumer got: " + slowConsumer.getMessageCount());
      System.out.println("fast consumer got: " + fastConsumer.getMessageCount());
      assertTrue(num + " messages should be all received within 10 sec, but: " + latch.getCount(), result);

      assertTrue(fastConsumer.getMessageCount() > slowConsumer.getMessageCount());
      session0.commit();
      session1.commit();
      //make sure queue is empty
      ClientSession.QueueQuery query0 = session0.queueQuery(new SimpleString("queue0"));
      ClientSession.QueueQuery query1 = session1.queueQuery(new SimpleString("queue0"));
      assertEquals(query0.getMessageCount(), 0);
      assertEquals(query1.getMessageCount(), 0);

      session0.close();
      session1.close();

      MessageRedistributionTest.log.info("Test done");
   }

   @Test
   public void testStarvationOnOneOfThreeNodes() throws Exception
   {
      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createStarvationAwareQueue(0, "queues.testaddress", "queue0", null, true);
      createStarvationAwareQueue(1, "queues.testaddress", "queue0", null, true);
      createStarvationAwareQueue(2, "queues.testaddress", "queue0", null, true);

      waitForBindings(0, "queues.testaddress", 2, 0, false);
      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      ClientSession session0 = addClientSession(sfs[0].createSession(true, true, 0));
      ClientSession session1 = addClientSession(sfs[1].createSession(true, true, 0));
      ClientSession session2 = addClientSession(sfs[2].createSession(true, true, 0));

      int num = 120;
      final CountDownLatch latch = new CountDownLatch(num);
      //consumer0 : 1 msg/sec
      FixedRateConsumer slowConsumer = createFixedRateConsumer(session0, "queue0", 1000, latch);
      //consumer1 : 20 msg/sec
      FixedRateConsumer fastConsumer1 = createFixedRateConsumer(session1, "queue0", 50L, latch);
      //consumer3 : 20 msg/sec
      FixedRateConsumer fastConsumer2 = createFixedRateConsumer(session2, "queue0", 50L, latch);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      //sending messages
      this.send(0, "queues.testaddress", num, false, null);

      boolean result = latch.await(60, TimeUnit.SECONDS);

      System.out.println("slow consumer got: " + slowConsumer.getMessageCount());
      System.out.println("fast consumer1 got: " + fastConsumer1.getMessageCount());
      System.out.println("fast consumer2 got: " + fastConsumer2.getMessageCount());
      assertTrue(num + " messages should be all received within 60 sec, but: " + latch.getCount(), result);

      assertTrue((fastConsumer1.getMessageCount() + fastConsumer2.getMessageCount()) / 2 > slowConsumer.getMessageCount());
      session0.commit();
      session1.commit();
      session2.commit();

      //make sure queue is empty
      ClientSession.QueueQuery query0 = session0.queueQuery(new SimpleString("queue0"));
      ClientSession.QueueQuery query1 = session1.queueQuery(new SimpleString("queue0"));
      ClientSession.QueueQuery query2 = session2.queueQuery(new SimpleString("queue0"));
      assertEquals(query0.getMessageCount(), 0);
      assertEquals(query1.getMessageCount(), 0);
      assertEquals(query2.getMessageCount(), 0);

      session0.close();
      session1.close();
      session2.close();

      MessageRedistributionTest.log.info("Test done");
   }

   @Test
   public void testStarvationOnTwoOfThreeNodes() throws Exception
   {
      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createStarvationAwareQueue(0, "queues.testaddress", "queue0", null, true);
      createStarvationAwareQueue(1, "queues.testaddress", "queue0", null, true);
      createStarvationAwareQueue(2, "queues.testaddress", "queue0", null, true);

      waitForBindings(0, "queues.testaddress", 2, 0, false);
      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      ClientSession session0 = addClientSession(sfs[0].createSession(true, true, 0));
      ClientSession session1 = addClientSession(sfs[1].createSession(true, true, 0));
      ClientSession session2 = addClientSession(sfs[2].createSession(true, true, 0));

      int num = 120;
      final CountDownLatch latch = new CountDownLatch(num);
      //consumer0 : 1 msg/sec
      FixedRateConsumer slowConsumer0 = createFixedRateConsumer(session0, "queue0", 1000L, latch);
      //consumer1 : 20 msg/sec
      FixedRateConsumer slowConsumer1 = createFixedRateConsumer(session1, "queue0", 1000L, latch);
      //consumer3 : 20 msg/sec
      FixedRateConsumer fastConsumer2 = createFixedRateConsumer(session2, "queue0", 50L, latch);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      //sending messages
      this.send(0, "queues.testaddress", num, false, null);

      boolean result = latch.await(60, TimeUnit.SECONDS);

      System.out.println("slow consumer0 got: " + slowConsumer0.getMessageCount());
      System.out.println("slow consumer1 got: " + slowConsumer1.getMessageCount());
      System.out.println("fast consumer2 got: " + fastConsumer2.getMessageCount());
      assertTrue(num + " messages should be all received within 60 sec, but: " + latch.getCount(), result);

      assertTrue((slowConsumer0.getMessageCount() + slowConsumer1.getMessageCount()) / 2 < fastConsumer2.getMessageCount());

      session0.commit();
      session1.commit();
      session2.commit();

      //make sure queue is empty
      ClientSession.QueueQuery query0 = session0.queueQuery(new SimpleString("queue0"));
      ClientSession.QueueQuery query1 = session1.queueQuery(new SimpleString("queue0"));
      ClientSession.QueueQuery query2 = session2.queueQuery(new SimpleString("queue0"));
      assertEquals(query0.getMessageCount(), 0);
      assertEquals(query1.getMessageCount(), 0);
      assertEquals(query2.getMessageCount(), 0);

      session0.close();
      session1.close();
      session2.close();

      MessageRedistributionTest.log.info("Test done");
   }

   @Test
   //one slow, one fast and one starvation unaware queue
   public void testStarvationWithNormalQueue() throws Exception
   {
      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createStarvationAwareQueue(0, "queues.testaddress", "queue0", null, true);
      createStarvationAwareQueue(1, "queues.testaddress", "queue0", null, true);
      createStarvationUnawareQueue(2, "queues.testaddress", "queue0", null, true);

      waitForBindings(0, "queues.testaddress", 2, 0, false);
      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      ClientSession session0 = addClientSession(sfs[0].createSession(true, true, 0));
      ClientSession session1 = addClientSession(sfs[1].createSession(true, true, 0));
      ClientSession session2 = addClientSession(sfs[2].createSession(true, true, 0));

      int num = 120;
      final CountDownLatch latch = new CountDownLatch(num);
      //consumer0 : 1 msg/sec
      FixedRateConsumer slowConsumer0 = createFixedRateConsumer(session0, "queue0", 1000L, latch);
      //consumer1 : 20 msg/sec
      FixedRateConsumer fastConsumer1 = createFixedRateConsumer(session1, "queue0", 50L, latch);
      //consumer3 : fast but starvation unaware
      FixedRateConsumer fastConsumer2 = createFixedRateConsumer(session2, "queue0", 10L, latch);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      //sending messages
      this.send(0, "queues.testaddress", num, false, null);

      boolean result = latch.await(60, TimeUnit.SECONDS);

      System.out.println("slow consumer0 got: " + slowConsumer0.getMessageCount());
      System.out.println("slow consumer1 got: " + fastConsumer1.getMessageCount());
      System.out.println("fast consumer2 got: " + fastConsumer2.getMessageCount());
      assertTrue(num + " messages should be all received within 60 sec, but: " + latch.getCount(), result);

      assertTrue(slowConsumer0.getMessageCount() < fastConsumer2.getMessageCount());
      assertEquals(40, fastConsumer2.getMessageCount());

      session0.commit();
      session1.commit();
      session2.commit();

      //make sure queue is empty
      ClientSession.QueueQuery query0 = session0.queueQuery(new SimpleString("queue0"));
      ClientSession.QueueQuery query1 = session1.queueQuery(new SimpleString("queue0"));
      ClientSession.QueueQuery query2 = session2.queueQuery(new SimpleString("queue0"));
      assertEquals(query0.getMessageCount(), 0);
      assertEquals(query1.getMessageCount(), 0);
      assertEquals(query2.getMessageCount(), 0);

      session0.close();
      session1.close();
      session2.close();

      MessageRedistributionTest.log.info("Test done");
   }

   @Test
   //two set of queues
   public void testStarvationWithDifferentQueues() throws Exception
   {
      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createStarvationAwareQueue(0, "queues.testaddress", "queue0", null, true);
      createStarvationAwareQueue(0, "queues.testaddress", "queue1", null, true);
      createStarvationAwareQueue(1, "queues.testaddress", "queue0", null, true);
      createStarvationAwareQueue(1, "queues.testaddress", "queue1", null, true);
      createStarvationAwareQueue(2, "queues.testaddress", "queue0", null, true);
      createStarvationAwareQueue(2, "queues.testaddress", "queue1", null, true);

      waitForBindings(0, "queues.testaddress", 4, 0, false);
      waitForBindings(0, "queues.testaddress", 2, 0, true);
      waitForBindings(1, "queues.testaddress", 4, 0, false);
      waitForBindings(1, "queues.testaddress", 2, 0, true);
      waitForBindings(2, "queues.testaddress", 4, 0, false);
      waitForBindings(2, "queues.testaddress", 2, 0, true);

      ClientSession session0 = addClientSession(sfs[0].createSession(true, true, 0));
      ClientSession session1 = addClientSession(sfs[1].createSession(true, true, 0));
      ClientSession session2 = addClientSession(sfs[2].createSession(true, true, 0));

      int num = 120;
      final CountDownLatch latch = new CountDownLatch(num * 2);

      FixedRateConsumer slowConsumer00 = createFixedRateConsumer(session0, "queue0", 1000L, latch);
      FixedRateConsumer fastConsumer01 = createFixedRateConsumer(session0, "queue1", 50L, latch);

      FixedRateConsumer fastConsumer10 = createFixedRateConsumer(session1, "queue0", 50L, latch);
      FixedRateConsumer slowConsumer11 = createFixedRateConsumer(session1, "queue1", 1000L, latch);

      FixedRateConsumer fastConsumer20 = createFixedRateConsumer(session2, "queue0", 10L, latch);

      waitForBindings(0, "queues.testaddress", 4, 3, false);
      waitForBindings(0, "queues.testaddress", 2, 2, true);
      waitForBindings(1, "queues.testaddress", 4, 3, false);
      waitForBindings(1, "queues.testaddress", 2, 2, true);
      waitForBindings(2, "queues.testaddress", 4, 4, false);
      waitForBindings(2, "queues.testaddress", 2, 1, true);

      //sending messages
      this.send(0, "queues.testaddress", num, false, null);

      boolean result = latch.await(240, TimeUnit.SECONDS);

      System.out.println("slow consumer00 got: " + slowConsumer00.getMessageCount());
      System.out.println("fast consumer10 got: " + fastConsumer10.getMessageCount());
      System.out.println("fast consumer20 got: " + fastConsumer20.getMessageCount());

      assertEquals(num, slowConsumer00.getMessageCount() + fastConsumer10.getMessageCount() + fastConsumer20.getMessageCount());
      System.out.println("fast consumer01 got: " + fastConsumer01.getMessageCount());
      System.out.println("slow consumer11 got: " + slowConsumer11.getMessageCount());

      assertEquals(num, fastConsumer01.getMessageCount() + slowConsumer11.getMessageCount());
      System.out.println("latch count: " + latch.getCount() + " result: " + result);
      assertTrue(num * 2 + " messages should be all received within 60 sec, but: " + latch.getCount(), result);

      assertTrue(slowConsumer11.getMessageCount() < fastConsumer01.getMessageCount());
      assertTrue((fastConsumer10.getMessageCount() + fastConsumer20.getMessageCount()) / 2 > slowConsumer00.getMessageCount());

      session0.commit();
      session1.commit();
      session2.commit();

      //make sure queue is empty
      ClientSession.QueueQuery query0 = session0.queueQuery(new SimpleString("queue0"));
      ClientSession.QueueQuery query1 = session1.queueQuery(new SimpleString("queue0"));
      ClientSession.QueueQuery query2 = session2.queueQuery(new SimpleString("queue0"));
      ClientSession.QueueQuery query3 = session0.queueQuery(new SimpleString("queue1"));
      ClientSession.QueueQuery query4 = session1.queueQuery(new SimpleString("queue1"));
      assertEquals(query0.getMessageCount(), 0);
      assertEquals(query1.getMessageCount(), 0);
      assertEquals(query2.getMessageCount(), 0);
      assertEquals(query3.getMessageCount(), 0);
      assertEquals(query4.getMessageCount(), 0);

      session0.close();
      session1.close();
      session2.close();

      MessageRedistributionTest.log.info("Test done");
   }

   @Test
   //fast consumer has a filter so message won't get redistributed
   public void testStarvationWithFilters() throws Exception
   {
      setupCluster(false);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      String filter = "color='red'";
      createStarvationAwareQueue(0, "queues.testaddress", "queue0", null, true);
      createStarvationAwareQueue(1, "queues.testaddress", "queue0", null, true);
      createStarvationAwareQueue(2, "queues.testaddress", "queue0", filter, true);

      waitForBindings(0, "queues.testaddress", 2, 0, false);
      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      ClientSession session0 = addClientSession(sfs[0].createSession(true, true, 0));
      ClientSession session1 = addClientSession(sfs[1].createSession(true, true, 0));
      ClientSession session2 = addClientSession(sfs[2].createSession(true, true, 0));

      int num = 30;
      final CountDownLatch latch = new CountDownLatch(num);
      //consumer0 : 1 msg/sec
      FixedRateConsumer slowConsumer0 = createFixedRateConsumer(session0, "queue0", 1000L, latch);
      //consumer1 : 1 msg/sec
      FixedRateConsumer slowConsumer1 = createFixedRateConsumer(session1, "queue0", 1000L, latch);
      //consumer3 : 20 msg/sec
      FixedRateConsumer fastConsumer2 = createFixedRateConsumer(session2, "queue0", 50L, latch);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      //sending messages
      this.send(0, "queues.testaddress", num, false, null);

      boolean result = latch.await(60, TimeUnit.SECONDS);

      System.out.println("slow consumer0 got: " + slowConsumer0.getMessageCount());
      System.out.println("slow consumer1 got: " + slowConsumer1.getMessageCount());
      System.out.println("fast consumer2 got: " + fastConsumer2.getMessageCount());
      assertTrue(num + " messages should be all received within 60 sec, but: " + latch.getCount(), result);

      //fast consumer don't get any because it has a filter.
      assertEquals(0, fastConsumer2.getMessageCount());
      assertEquals(num / 2, slowConsumer0.getMessageCount());
      assertEquals(num / 2, slowConsumer1.getMessageCount());

      session0.commit();
      session1.commit();
      session2.commit();

      //make sure queue is empty
      ClientSession.QueueQuery query0 = session0.queueQuery(new SimpleString("queue0"));
      ClientSession.QueueQuery query1 = session1.queueQuery(new SimpleString("queue0"));
      ClientSession.QueueQuery query2 = session2.queueQuery(new SimpleString("queue0"));
      assertEquals(query0.getMessageCount(), 0);
      assertEquals(query1.getMessageCount(), 0);
      assertEquals(query2.getMessageCount(), 0);

      session0.close();
      session1.close();
      session2.close();

      MessageRedistributionTest.log.info("Test done");
   }

   private FixedRateConsumer createFixedRateConsumer(ClientSession session, String queueName,
         long rate, CountDownLatch latch) throws HornetQException
   {
      ClientConsumer coreConsumer = addClientConsumer(session.createConsumer(queueName, null, 0, -1, false));

      session.start();

      FixedRateConsumer consumer = new FixedRateConsumer(latch, rate);

      coreConsumer.setMessageHandler(consumer);

      return consumer;
   }

   private void createStarvationAwareQueue(int node, String address,
         String name, String filter, boolean durable) throws Exception
   {
      SimpleString filterVal = filter == null ? null : new SimpleString(filter);
      servers[node].createQueue(new SimpleString(address), new SimpleString(name), filterVal, durable, false, false, true);
   }

   private void createStarvationUnawareQueue(int node, String address,
                                           String name, String filter, boolean durable) throws Exception
   {
      SimpleString filterVal = filter == null ? null : new SimpleString(filter);
      servers[node].createQueue(new SimpleString(address), new SimpleString(name), filterVal, durable, false);
   }

   protected void setupCluster(final boolean forwardWhenNoConsumers) throws Exception
   {
      setupClusterConnection("cluster0", "queues", forwardWhenNoConsumers, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", forwardWhenNoConsumers, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", forwardWhenNoConsumers, 1, isNetty(), 2, 0, 1);
   }

   protected void setRedistributionDelay(final long delay)
   {
      AddressSettings as = new AddressSettings();
      as.setRedistributionDelay(delay);

      getServer(0).getAddressSettingsRepository().addMatch("queues.*", as);
      getServer(1).getAddressSettingsRepository().addMatch("queues.*", as);
      getServer(2).getAddressSettingsRepository().addMatch("queues.*", as);
   }

   protected void setupServers() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());
   }

   protected void stopServers() throws Exception
   {
      closeAllConsumers();

      closeAllSessionFactories();

      closeAllServerLocatorsFactories();

      stopServers(0, 1, 2);

      clearServer(0, 1, 2);
   }

   private static class FixedRateConsumer implements MessageHandler
   {
      private CountDownLatch latch;
      private long msPerMsg;
      private int count = 0;

      public FixedRateConsumer(CountDownLatch latch, long msPerMsg)
      {
         this.latch = latch;
         this.msPerMsg = msPerMsg;
      }

      @Override
      public void onMessage(ClientMessage message)
      {
         try
         {
            try
            {
               message.acknowledge();
               count++;
            }
            catch (HornetQException e)
            {
               System.err.println("failed to ack message, count won't be correct.");
            }
            Thread.sleep(msPerMsg);
         }
         catch (InterruptedException e)
         {
         }
         latch.countDown();
      }

      public int getMessageCount()
      {
         return count;
      }
   }
}
