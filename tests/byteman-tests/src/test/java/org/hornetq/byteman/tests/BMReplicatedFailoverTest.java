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

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationStartSyncMessage;
import org.hornetq.tests.integration.cluster.failover.FailoverTestBase;
import org.hornetq.tests.integration.cluster.failover.ReplicatedFailoverTest;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
@RunWith(BMUnitRunner.class)
public class BMReplicatedFailoverTest extends ReplicatedFailoverTest
{
   private static BMReplicatedFailoverTest testInstance;
   private CountDownLatch receiveAckLatch = new CountDownLatch(1);
   private CountDownLatch sendCommitLatch = new CountDownLatch(1);
   private CountDownLatch actionEndLatch = new CountDownLatch(1);

   private FailoverControl failoverControl = new FailoverControl();
   private String repNodeId;

   public static void checkReplicationPacket(ReplicationStartSyncMessage packet)
   {
      if (packet.getDataType() != null)
      {
         if (packet.getDataType() == ReplicationStartSyncMessage.SyncDataType.JournalMessages && (!packet.isServerToFailBack()))
         {
            testInstance.doSendCommit();
            testInstance.setRepNodeId(packet.getNodeID());
            testInstance.waitForReturn();
         }
      }
   }

   private void waitForReturn()
   {
      try
      {
         actionEndLatch.await(200, TimeUnit.MILLISECONDS);
      }
      catch (InterruptedException e)
      {
         e.printStackTrace();
      }
   }

   private void setRepNodeId(String nid)
   {
      this.repNodeId = nid;
   }

   public static void doConsumerAck(String nodeID)
   {
      if (nodeID.equals(testInstance.repNodeId))
      {
         testInstance.receiveAckLatch.countDown();
      }
   }

   @Test
   /**
    * This test does the following:
    * 1. Setup and starting a live-backup in replication mode (setup)
    * 2. create a producer thread and consumer threads (both sessions are transactional)
    * 3. crash the live server, backup will become live
    * 4. producer thread sends a number of messages, but doesn't commit
    * 5. restart the live server and let the failback happen
    * 6. during the failback synchronization process let the producer thread commit the session
    * 7. then let the consumer immediately receive messages and cause a blocking batch ack happen
    *
    * We using a byteman rule to create a situation during step 6 so that the producer
    * commit result in a rollback on the server, and this rollback will cause a
    * UNSUPPORTED_PACKET type exception at backup's ReplicationEndpoint.
    *
    * This will leave one pending context in ReplicationManager's pendingTokens queue
    * un-answered. Due to this, when the consumer does a blocking batch ack, it cannot
    * get back the response because the OperationContext where the response is scheduled
    * will never gets its replicationLineUp match the replicated value. So the blocking
    * Ack will result in timeout (30 sec default).
    *
    * This is a special case but it shows a generic problem, i.e. in case of some exceptions
    * during the replication synchronization, some pending token (replication packet) will
    * not get answered (i.e. ReplicationManager.replicated() is not called if exception
    * happens), It creates a situation where any tasks replying on replication lineup
    * may get blocked.
    *
    */
   @BMRules
      (
         rules =
            {
               @BMRule
                  (
                     name = "send commit control",
                     targetClass = "org.hornetq.core.replication.ReplicationEndpoint",
                     targetMethod = "handleStartReplicationSynchronization",
                     targetLocation = "AT EXIT",
                     action = "org.hornetq.byteman.tests.BMReplicatedFailoverTest.checkReplicationPacket($1)"
                  ),
               @BMRule
               (
                      name = "consumer ack control",
                      targetClass = "org.hornetq.core.replication.ReplicationEndpoint",
                      targetMethod = "finishSynchronization",
                      targetLocation = "AT EXIT",
                      action = "org.hornetq.byteman.tests.BMReplicatedFailoverTest.doConsumerAck($1)"
               )
            }
      )
   public void testReplicationLineupTimeout() throws Exception
   {
      testInstance = this;
      try
      {
         backupServer.getServer().getConfiguration().setFailbackDelay(2000);
         backupServer.getServer().getConfiguration().setMaxSavedReplicatedJournalSize(2);
         //send some messages enough for consumer to send an ack
         //default batch ack size DEFAULT_ACK_BATCH_SIZE = 1024 * 1024;
         createBlockOnAckSessionFactory();

         createSessionAndQueue().close();

         ClientSession sendSession = createSession(sf, false, false);

         ClientProducer producer = addClientProducer(sendSession.createProducer(FailoverTestBase.ADDRESS));

         final int num1 = 100, num2 = 10;
         sendTextMessages(sendSession, producer, num1, 1024 * 20);

         sendSession.commit();

         ProducerControl producerControl = new ProducerControl(sendSession, producer, num2);

         ClientSession receiveSession = createSession(sf, false, false);

         ClientConsumer consumer = receiveSession.createConsumer(FailoverTestBase.ADDRESS);
         receiveSession.start();

         failoverControl.addSession(sendSession);
         failoverControl.addSession(receiveSession);

         //producer will send some messages, wait for failback
         //then commit, which will cause replicationEndpoint
         //to respond with a UNSUPPORTED_PACKET exception
         producerControl.start();

         ConsumerControl consumerControl = new ConsumerControl(receiveSession, consumer, num1);

         //consumer will wait for producer causing the UNSUPPORTED_PACKET exception
         //then send out the batch ack, which (if the exception is not properly
         //handled) will block until timeout.
         consumerControl.start();

         producerControl.waitForResult();
         //no timeout happens and messages are received.
         consumerControl.waitForResult();
      }
      catch (Exception e)
      {
         throw e;
      }
      finally
      {
         testInstance = null;
      }
   }

   private ClientSession createSessionAndQueue() throws Exception
   {
      ClientSession session = createSession(sf, false, false);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);
      return session;
   }

   public void doSendCommit()
   {
      sendCommitLatch.countDown();
   }

   private void waitForConsumerAckSignal()
   {
      try
      {
         receiveAckLatch.await(60, TimeUnit.SECONDS);
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
      private volatile Throwable exception;
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
                  failoverControl.doFailover();

                  for (int i = 0; i < number; i++)
                  {
                     ClientMessage m = sendSession.createMessage(true);
                     producer.send(m);
                  }

                  failoverControl.doFailback();

                  //commit
                  try
                  {
                     sendCommitLatch.await(60, TimeUnit.SECONDS);
                     //send one more to make the TransactionImpl.containsPersistent to true
                     ClientMessage m = sendSession.createMessage(true);
                     producer.send(m);

                     sendSession.commit();
                  }
                  catch (HornetQException e)
                  {
                     if (e.getType() == HornetQExceptionType.TRANSACTION_ROLLED_BACK)
                     {
                        System.out.println("got expected exception!!" + e);
                     }
                     else
                     {
                        exception = e;
                     }
                  }
                  finally
                  {
                     //if it goes here it means the bug is fixed.
                     //release the latch in case it still blocks the byteman rule call
                     actionEndLatch.countDown();
                  }
               }
               catch (Throwable e)
               {
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
            sendThread.join(60000);
         }
         catch (InterruptedException e)
         {
         }
         if (!finishedNormally)
         {
            throw new Exception("Producer didn't finish in 30 sec");
         }
         if (exception != null)
         {
            throw new Exception("Producer got exception: " + exception);
         }
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
                     m.acknowledge();
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
                  e.printStackTrace();
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
            receiveThread.join(60000);
         }
         catch (InterruptedException e)
         {
         }
         if (!finishedNormally)
         {
            throw new Exception("Consumer didn't finish in 60 sec");
         }
         if (errorMessage != null)
         {
            throw new Exception("Consumer got error: " + errorMessage);
         }
      }

   }

   private class FailoverControl
   {
      private List<ClientSession> sessions = new ArrayList<ClientSession>();

      public void addSession(ClientSession s)
      {
         synchronized (sessions)
         {
            sessions.add(s);
         }
      }

      public ClientSession[] getSessions()
      {
         synchronized (sessions)
         {
            return sessions.toArray(new ClientSession[0]);
         }
      }

      public void doFailover() throws Exception
      {
         crash(getSessions());
      }

      private void doFailback() throws Exception
      {
         Thread t = new Thread() {
            public void run()
            {
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
                  t.printStackTrace();
               }
            }
         };
         t.start();
      }
   }
}
