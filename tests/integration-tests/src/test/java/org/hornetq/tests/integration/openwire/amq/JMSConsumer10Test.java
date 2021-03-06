/*
 * Copyright 2005-2014 Red Hat, Inc.
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
package org.hornetq.tests.integration.openwire.amq;

import java.util.Arrays;
import java.util.Collection;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.command.ActiveMQDestination;
import org.hornetq.tests.integration.openwire.BasicOpenWireTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * adapted from: org.apache.activemq.JMSConsumerTest
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 *
 */
@RunWith(Parameterized.class)
public class JMSConsumer10Test extends BasicOpenWireTest
{
   @Parameters
   public static Collection<Object[]> getParams()
   {
      return Arrays.asList(new Object[][] {
         {DeliveryMode.NON_PERSISTENT, Session.AUTO_ACKNOWLEDGE, ActiveMQDestination.QUEUE_TYPE},
         {DeliveryMode.NON_PERSISTENT, Session.DUPS_OK_ACKNOWLEDGE, ActiveMQDestination.QUEUE_TYPE},
         {DeliveryMode.NON_PERSISTENT, Session.CLIENT_ACKNOWLEDGE, ActiveMQDestination.QUEUE_TYPE},
         {DeliveryMode.PERSISTENT, Session.AUTO_ACKNOWLEDGE, ActiveMQDestination.QUEUE_TYPE},
         {DeliveryMode.PERSISTENT, Session.DUPS_OK_ACKNOWLEDGE, ActiveMQDestination.QUEUE_TYPE},
         {DeliveryMode.PERSISTENT, Session.CLIENT_ACKNOWLEDGE, ActiveMQDestination.QUEUE_TYPE}
      });
   }

   public int deliveryMode;
   public int ackMode;
   public byte destinationType;

   public JMSConsumer10Test(int deliveryMode, int ackMode, byte destinationType)
   {
      this.deliveryMode = deliveryMode;
      this.ackMode = ackMode;
      this.destinationType = destinationType;
   }

   @Test
   public void testUnackedWithPrefetch1StayInQueue() throws Exception
   {

      // Set prefetch to 1
      connection.getPrefetchPolicy().setAll(1);
      connection.start();

      // Use all the ack modes
      Session session = connection.createSession(false, ackMode);
      ActiveMQDestination destination = createDestination(session, destinationType);
      MessageConsumer consumer = session.createConsumer(destination);

      // Send the messages
      sendMessages(session, destination, 4);

      // Only pick up the first 2 messages.
      Message message = null;
      for (int i = 0; i < 2; i++)
      {
         message = consumer.receive(1000);
         assertNotNull(message);
      }
      message.acknowledge();

      connection.close();
      connection = (ActiveMQConnection) factory.createConnection();
      connection.getPrefetchPolicy().setAll(1);
      connection.start();

      // Use all the ack modes
      session = connection.createSession(false, ackMode);
      consumer = session.createConsumer(destination);

      // Pickup the rest of the messages.
      for (int i = 0; i < 2; i++)
      {
         message = consumer.receive(1000);
         assertNotNull(message);
      }
      message.acknowledge();
      assertNull(consumer.receiveNoWait());

   }

}
