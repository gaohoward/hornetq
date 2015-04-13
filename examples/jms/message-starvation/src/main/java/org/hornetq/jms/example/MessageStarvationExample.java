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
package org.hornetq.jms.example;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.hornetq.common.example.HornetQExample;

/**
 * A simple example that demonstrates how the broker can handle
 * message starvation situation.
 *
 * @author <a href="hgao@redhat.com>Howard Gao</a>
 */
public class MessageStarvationExample extends HornetQExample
{
   public static void main(final String[] args)
   {
      new MessageStarvationExample().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      Connection connection0 = null;

      Connection connection1 = null;

      InitialContext ic0 = null;

      InitialContext ic1 = null;

      try
      {
         // Step 1. Get an initial context for looking up JNDI from server 0
         ic0 = getContext(0);

         // Step 2. Look-up the JMS Queue objects from JNDI
         Queue starvationAwareQueue = (Queue)ic0.lookup("/queue/starvationAwareQueue");
         Queue normalQueue = (Queue)ic0.lookup("/queue/normalQueue");

         // Step 3. Look-up a JMS Connection Factory object from JNDI on server 0
         ConnectionFactory cf0 = (ConnectionFactory)ic0.lookup("/ConnectionFactory");

         // Step 4. Get an initial context for looking up JNDI from server 1
         ic1 = getContext(1);

         // Step 5. Look-up a JMS Connection Factory object from JNDI on server 1
         ConnectionFactory cf1 = (ConnectionFactory)ic1.lookup("/ConnectionFactory");

         // Step 6. We create a JMS Connection connection0 which is a connection to server 0
         connection0 = cf0.createConnection();

         // Step 7. We create a JMS Connection connection1 which is a connection to server 1
         connection1 = cf1.createConnection();

         // Step 8. We create 2 JMS Sessions on server 0
         Session session0 = connection0.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session session1 = connection0.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 9. We create 2 JMS Session on server 1
         Session session2 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session session3 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 10. We create a JMS MessageProducer object on server 0 to the normal queue
         // and create another on server 0 to the starvation aware queue.
         MessageProducer producerN = session0.createProducer(normalQueue);
         MessageProducer producerS = session0.createProducer(starvationAwareQueue);

         // Step 11. We send some messages to server 0 to both queues
         final int numMessages = 20;

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage message = session0.createTextMessage("This is text message " + i);
            producerN.send(message);
            producerS.send(message);
         }

         // Step 12. We use latches to count the messages to be received.
         final CountDownLatch latchN = new CountDownLatch(numMessages);
         final CountDownLatch latchS = new CountDownLatch(numMessages);
         final AtomicLong finishTimeN = new AtomicLong(0L);
         final AtomicLong finishTimeS = new AtomicLong(0L);

         // Step 13. We create JMS MessageConsumer objects on server 0 and server 1
         // slow consumers consume messages at rate one message per second
         // fast consumers consume messages at rate 25 messages per second
         MessageConsumer slowConsumerNormalQueue0 = session0.createConsumer(normalQueue);
         slowConsumerNormalQueue0.setMessageListener(new MessageCountListener(latchN, 1000, finishTimeN));
         MessageConsumer slowConsumerStarvationAwareQueue0 = session1.createConsumer(starvationAwareQueue);
         slowConsumerStarvationAwareQueue0.setMessageListener(new MessageCountListener(latchS, 1000, finishTimeS));

         MessageConsumer fastConsumerNormalQueue1 = session2.createConsumer(normalQueue);
         fastConsumerNormalQueue1.setMessageListener(new MessageCountListener(latchN, 25, finishTimeN));
         MessageConsumer fastConsumerStarvationAwareQueue1 = session3.createConsumer(starvationAwareQueue);
         fastConsumerStarvationAwareQueue1.setMessageListener(new MessageCountListener(latchS, 25, finishTimeS));

         // Step 14. We start the connections to start delivery
         long beginTime = System.currentTimeMillis();
         connection0.start();
         connection1.start();
         
         latchS.await();
         latchN.await();

         while (finishTimeN.get() == 0L || finishTimeS.get() == 0L)
         {
            Thread.sleep(5);
         }
         long timeS = finishTimeS.get() - beginTime;
         long timeN = finishTimeN.get() - beginTime;
         
         System.out.println("Total consumption time at normal queue: " + timeN);
         System.out.println("Total consumption time at starvation aware queue: " + timeS);

         return timeN > timeS;
      }
      finally
      {
         // Step 15. Be sure to close our resources!

         if (connection0 != null)
         {
            connection0.close();
         }

         if (connection1 != null)
         {
            connection1.close();
         }

         if (ic0 != null)
         {
            ic0.close();
         }

         if (ic1 != null)
         {
            ic1.close();
         }
      }
   }

   private static class MessageCountListener implements MessageListener
   {
      private CountDownLatch latch;
      private long rate;
      private AtomicLong finishTime;

      public MessageCountListener(CountDownLatch latch, long rate, AtomicLong finishTime)
      {
         this.latch = latch;
         this.rate = rate;
         this.finishTime = finishTime;
      }

      @Override
      public void onMessage(Message m)
      {
         try
         {
            Thread.sleep(rate);
         }
         catch (InterruptedException e)
         {
            e.printStackTrace();
         }
         latch.countDown();
         if (latch.getCount() == 0L)
         {
            finishTime.set(System.currentTimeMillis());
         }
      }
   }
}
