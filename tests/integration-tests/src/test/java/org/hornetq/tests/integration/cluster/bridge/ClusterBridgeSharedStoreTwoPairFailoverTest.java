/*
 * Copyright 2013 Red Hat, Inc.
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
package org.hornetq.tests.integration.cluster.bridge;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.integration.cluster.util.MultiServerTestBase;
import org.junit.Test;

import java.util.UUID;

public class ClusterBridgeSharedStoreTwoPairFailoverTest extends MultiServerTestBase
{

   final String TEST_QUEUE = "cluster-queues";

   @Override
   protected int getNumberOfServers()
   {
      return 2;
   }

   @Override
   protected boolean useNetty()
   {
      return true;
   }


   @Test
   public void testKillAllLiveServers() throws Exception
   {
      for (HornetQServer server : servers)
      {
         server.getConfiguration().getQueueConfigurations().add(new CoreQueueConfiguration(TEST_QUEUE, TEST_QUEUE, null, true));
         AddressSettings as = new AddressSettings();
         as.setRedistributionDelay(0);
         server.getConfiguration().getAddressesSettings().put("#", as);
      }

      for (HornetQServer server : backupServers)
      {
         server.getConfiguration().getQueueConfigurations().add(new CoreQueueConfiguration(TEST_QUEUE, TEST_QUEUE, null, true));
         AddressSettings as = new AddressSettings();
         as.setRedistributionDelay(0);
         server.getConfiguration().getAddressesSettings().put("#", as);
      }

      startServers();

      ServerLocator[] locators = new ServerLocator[2];
      ClientSessionFactory[] sfs = new ClientSessionFactory[2];
      ClientSession[] sessions = new ClientSession[2];

      for (int i = 0; i < sessions.length; i++)
      {
         locators[i] = createLocator(true, i);
         locators[i].setReconnectAttempts(-1);
         sfs[i] = createSessionFactory(locators[i]);
         sessions[i] = sfs[i].createSession(true, true);
         sessions[i].start();
      }

      //make sure cluster bridge works
      sendReceiveMessage(sessions[0], sessions[1]);
      sendReceiveMessage(sessions[1], sessions[0]);

      servers[0].stop(true);
      waitForServerToStop(servers[0]);

      //make sure cluster bridge works
      sendReceiveMessage(sessions[0], sessions[1]);
      sendReceiveMessage(sessions[1], sessions[0]);

      servers[1].stop(true);
      waitForServerToStop(servers[1]);

      //make sure cluster bridge works
      sendReceiveMessage(sessions[0], sessions[1]);
      sendReceiveMessage(sessions[1], sessions[0]);

      backupServers[0].stop(false);
      backupServers[1].stop(false);

   }

   private void sendReceiveMessage(ClientSession sendSession, ClientSession receiveSession) throws HornetQException
   {

      ClientConsumer consumer = receiveSession.createConsumer(TEST_QUEUE);
      ClientProducer producer = null;
      int retryCount = 20;
      final String mid = UUID.randomUUID().toString();
      while (retryCount > 0)
      {
         try
         {
            producer = sendSession.createProducer(TEST_QUEUE);

            ClientMessage m = sendSession.createMessage(true);
            m.putStringProperty("myid", mid);
            producer.send(m);
            break;
         }
         catch (HornetQException e)
         {
            retryCount--;
         }
         finally
         {
            if (producer != null)
            {
               producer.close();
            }
         }
      }
      ClientMessage m = consumer.receive(5000);
      assertNotNull(m);
      m.acknowledge();
      String myId = m.getStringProperty("myid");
      assertEquals(mid, myId);
      consumer.close();
   }
}
