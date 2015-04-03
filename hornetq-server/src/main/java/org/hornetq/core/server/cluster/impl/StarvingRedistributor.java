package org.hornetq.core.server.cluster.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.HandleStatus;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.cluster.RemoteQueueBinding;
import org.hornetq.core.server.impl.QueueImpl;

public class StarvingRedistributor extends Redistributor
{
   private final Map<Long, RemoteQueueBinding> starvingMap;
   private AtomicLong quota = new AtomicLong(0);

   public StarvingRedistributor(QueueImpl queueImpl,
         StorageManager storageManager, PostOffice postOffice,
         Executor executor, int redistributorBatchSize)
   {
      super(queueImpl, storageManager, postOffice, executor, redistributorBatchSize);
      this.starvingMap = new ConcurrentHashMap<Long, RemoteQueueBinding>();
   }

   @Override
   public HandleStatus handle(final MessageReference reference) throws Exception
   {
      if (quota.get() <= 0)
      {
         return HandleStatus.BUSY;
      }

      HandleStatus status = super.handle(reference);

      if (status == HandleStatus.HANDLED)
      {
         quota.decrementAndGet();
      }
      return status;
   }

   public void resetQuota(Binding newBinding)
   {
      if (!starvingMap.containsKey(newBinding.getID()))
      {
         starvingMap.put(newBinding.getID(), (RemoteQueueBinding) newBinding);
      }

      //re-calculate quota = total messages divided by total consumers, local
      //consumers are considered one.
      long mcnt = queue.getMessageCount(0);

      quota.set(mcnt / (starvingMap.size() + 1) * starvingMap.size());
   }

   public boolean accept(Binding binding)
   {
      if (binding instanceof RemoteQueueBinding)
      {
         if (starvingMap.containsKey(binding.getID()))
         {
            return true;
         }
      }
      return false;
   }

   @Override
   public synchronized void stop()
   {
      super.stop();
      starvingMap.clear();
   }

}
