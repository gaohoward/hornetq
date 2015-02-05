/*
* JBoss, Home of Professional Open Source.
* Copyright 2010, Red Hat, Inc., and individual contributors
* as indicated by the @author tags. See the copyright.txt file in the
* distribution for a full listing of individual contributors.
*
* This is free software; you can redistribute it and/or modify it
* under the terms of the GNU Lesser General Public License as
* published by the Free Software Foundation; either version 2.1 of
* the License, or (at your option) any later version.
*
* This software is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this software; if not, write to the Free
* Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
* 02110-1301 USA, or see the FSF site: http://www.fsf.org.
*/
package org.hornetq.byteman.tests;

import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.tests.integration.cluster.failover.FailoverTestBase;
import org.hornetq.tests.integration.cluster.failover.ReplicatedFailoverTest;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public class BMReplicatedFailoverTest extends ReplicatedFailoverTest
{
   private CountDownLatch latch = new CountDownLatch(1);

   public static void checkReplicationPacket(Packet packet)
   {
      if (packet.getType() == PacketImpl.REPLICATION_COMMIT_ROLLBACK)
      {
         System.out.println("we can be sure the test has triggered this: " + packet);
      }
   }

   @Test
   @BMRules
      (
         rules =
            {
               @BMRule
                  (
                     name = "debug",
                     targetClass = "org.hornetq.core.replication.ReplicationEndpoint",
                     targetMethod = "handlePacket",
                     targetLocation = "ENTRY",
                     action = "org.hornetq.byteman.tests.BMReplicatedFailoverTest.checkReplicationPacket($1)"
                  )
            }
      )
   public void testReplicationLineupTimeout() throws Exception
   {
      backupServer.getServer().getConfiguration().setFailbackDelay(2000);
      backupServer.getServer().getConfiguration().setMaxSavedReplicatedJournalSize(2);
      //send some messages enough for consumer to send an ack
      //default batch ack size DEFAULT_ACK_BATCH_SIZE = 1024 * 1024;
      createSessionFactory();
      ClientSession session = createSessionAndQueue();

      ClientSession sendSession = createSession(sf, false, false);

      ClientProducer producer = addClientProducer(sendSession.createProducer(FailoverTestBase.ADDRESS));

      final int num1 = 100, num2 = 10;
      sendTextMessages(sendSession, producer, num1, 1024 * 20);

      sendSession.commit();

      System.out.println("------now some messages sent " + num1);
      ProducerControl producerControl = new ProducerControl(sendSession, producer, num2);

      //producer will send some messages, wait for failback
      //then commit, which will cause replicationEndpoint
      //to respond with a UNSUPPORTED_PACKET exception
      producerControl.start();

      ClientSession receiveSession = createSession(sf, false, false);
      ClientConsumer consumer = receiveSession.createConsumer(FailoverTestBase.ADDRESS);
      receiveSession.start();

      ConsumerControl consumerControl = new ConsumerControl(receiveSession, consumer, num1);

      //consumer will wait for producer causing the UNSUPPORTED_PACKET exception
      //then send out the batch ack, which (if the exception is not properly
      //handled) will block until timeout.
      consumerControl.start();

      producerControl.waitForResult();
      //no timeout happens and messages are received.
      consumerControl.waitForResult();

      System.out.println("------------------test end.");
   }

   private ClientSession createSessionAndQueue() throws Exception
   {
      ClientSession session = createSession(sf, false, false);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);
      return session;
   }

   private void doFailover() throws Exception
   {
      ClientSession session = createSession(sf, true, true);
      crash(session);
      System.out.println("now live server crashed, so failover will happen.");
   }

   private void doFailback() throws Exception
   {
      System.out.println("----------now fail back!!");
      try
      {
         liveServer.getServer().getConfiguration().setCheckForLiveServer(true);

         liveServer.start();

         waitForRemoteBackupSynchronization(liveServer.getServer());

         waitForRemoteBackupSynchronization(backupServer.getServer());

         waitForServer(liveServer.getServer());
      }
      catch (Throwable t)
      {
         System.err.println("--------------got exception " + t);
         t.printStackTrace();
      }
      System.out.println("-------------fail back ok.");
   }

   private void doConsumerAck()
   {
      latch.countDown();
   }

   private void waitForConsumerAckSignal()
   {
      try
      {
         latch.await();
      }
      catch (InterruptedException e)
      {
         e.printStackTrace();
      }
   }

   private class ProducerControl
   {
      private ClientSession sendSession;
      private ClientProducer producer;
      private int number;
      private volatile Exception exception;
      private volatile boolean finishedNormally;
      private Thread sendThread;

      public ProducerControl(ClientSession sendSession, ClientProducer producer, int number)
      {
         this.sendSession = sendSession;
         this.producer = producer;
         this.number = number;
      }

      public void start()
      {
         sendThread = new Thread()
         {
            @Override
            public void run()
            {
               try
               {
                  doFailover();
                  for (int i = 0; i < number; i++)
                  {
                     ClientMessage m = sendSession.createMessage(true);
                     producer.send(m);
                  }

                  doFailback();
                  System.out.println("--------------------dofailback done");
                  //commit
                  sendSession.commit();
                  //let consumer to start receiving
                  //may not be here if the commit returns with some exception
                  //if so do it in the catch block
                  doConsumerAck();
               }
               catch (Exception e)
               {
                  System.out.println("--------------------producer got exception " + e);
                  exception = e;
               }
               finally
               {
                  finishedNormally = true;
               }
            }
         };
         sendThread.start();
      }

      public void waitForResult() throws Exception
      {
         try
         {
            sendThread.join(10000);
         }
         catch (InterruptedException e)
         {
         }
         if (!finishedNormally)
         {
            throw new Exception("Producer didn't finish in 10 sec");
         }
         if (exception != null)
         {
            throw new Exception("Producer got exception: " + exception);
         }
         System.out.println("producer works fine!");
      }
   }

   private class ConsumerControl
   {
      private ClientSession receiveSession;
      private ClientConsumer consumer;
      private int number;
      private volatile String errorMessage;
      private volatile boolean finishedNormally;
      private Thread receiveThread;

      public ConsumerControl(ClientSession receiveSession, ClientConsumer consumer, int number)
      {
         this.receiveSession = receiveSession;
         this.consumer = consumer;
         this.number = number;
      }

      public void start()
      {
         receiveThread = new Thread()
         {
            @Override
            public void run()
            {
               try
               {
                  waitForConsumerAckSignal();
                  for (int i = 0; i < number; i++)
                  {
                     ClientMessage m = consumer.receive(5000);
                     if (m == null)
                     {
                        errorMessage = "null message got " + i;
                        break;
                     }
                  }
                  //commit
                  receiveSession.commit();
                  ClientMessage m = consumer.receive(5000);
                  if (m != null && errorMessage == null)
                  {
                     errorMessage = "we have extra messages received";
                  }
               }
               catch (Exception e)
               {
                  errorMessage = "we got exception receiving: " + e;
               }
               finally
               {
                  finishedNormally = true;
               }
            }
         };
         receiveThread.start();
      }

      public void waitForResult() throws Exception
      {
         try
         {
            receiveThread.join(10000);
         }
         catch (InterruptedException e)
         {
         }
         if (!finishedNormally)
         {
            throw new Exception("Consumer didn't finish in 10 sec");
         }
         if (errorMessage != null)
         {
            throw new Exception("Consumer got error: " + errorMessage);
         }
         System.out.println("consumer works fine!");
      }

   }

}
