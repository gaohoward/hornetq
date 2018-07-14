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
package org.hornetq.utils;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DebugLogger
{
   private SimpleDateFormat format = new SimpleDateFormat("MMdd-HH");

   boolean inited = false;

   public String logFileName;
   public String fileRealPath;

   public static String VMID;

   static
   {
      long curTim = System.currentTimeMillis();
      long val = curTim / 100;
      long val2 = val / 100000;
      long val3 = val2 * 100000;
      long v = val % val3;
      VMID = String.valueOf(v);
   }

   private static final Map<String, DebugLogger> loggers = new HashMap<String, DebugLogger>();

   public static final DebugLogger getLogger(String fname)
   {
      DebugLogger logger = null;
      synchronized (loggers)
      {
         logger = loggers.get(fname);
         if (logger == null)
         {
            logger = new DebugLogger(fname + "-" + VMID);
            loggers.put(fname, logger);
         }
      }
      return logger;
   }

   private DebugLogger(String fname)
   {
      logFileName = fname + "-" + VMID;
      init();
   }

   private void init()
   {
      if (inited)
         return;
      String udir = System.getProperty("user.home");
      File baseFile = new File(udir);

      Date d = new Date();
      String today = format.format(d);
      String baseDir = "debug-log-" + today;

      File dirFile = new File(baseFile, baseDir);

      if (!dirFile.exists())
      {
         dirFile.mkdir();
      }

      File logFile = new File(dirFile, logFileName);
      fileRealPath = logFile.getAbsolutePath();
      inited = true;
   }

   public void log(String rec)
   {
      log(rec, false, null);
   }

   public void log(String rec, boolean stack)
   {
      log(rec, stack, null);
   }

   public void log(String rec, boolean printStack,
         Throwable t)
   {
      ControlledPrintWriter writer = WriterUtil.getWriter(fileRealPath);
      if (writer != null)
      {
         writer.writeRecord(rec, printStack, t);
      }
      else
      {
         throw new IllegalStateException("Couldn't obtain a writer " + fileRealPath);
      }
   }

   static class WriterUtil
   {
      public static HashMap<String, ControlledPrintWriter> allWriters = new HashMap<String, ControlledPrintWriter>();

      public static synchronized ControlledPrintWriter getWriter(String path)
      {
         ControlledPrintWriter writer = allWriters.get(path);
         try
         {
            if (writer == null)
            {
               writer = new ControlledPrintWriter(path, true);
               allWriters.put(path, writer);
            }
         }
         catch (FileNotFoundException e)
         {
            writer = null;
         }
         return writer;
      }
   }

   static class ControlledPrintWriter
   {
      private PrintWriter internalWriter;
      private String filePath;
      private boolean autoFlush;
      private int ln_counter;
      private byte f_counter;

      private static final int FILE_LINES = 800000;

      // private static final int FILE_LINES = 10;

      public ControlledPrintWriter(String path, boolean flush)
         throws FileNotFoundException
      {
         ln_counter = 0;
         f_counter = 0;
         filePath = path;
         autoFlush = flush;
         internalWriter = new PrintWriter(new FileOutputStream(filePath, true),
               autoFlush);
      }

      // I'm not thread safe, whoever uses me takes care of it.
      private void println(String message)
      {
         internalWriter.println(message);
         ln_counter++;
         if (ln_counter >= FILE_LINES)
         {
            internalWriter.close();
            // back up
            File oldFile = new File(filePath);
            File bkFile = new File(filePath + f_counter % 100);
            f_counter++;

            if (bkFile.exists())
            {
               bkFile.delete();
            }
            oldFile.renameTo(bkFile);

            // startNew
            try
            {
               internalWriter = new PrintWriter(new FileOutputStream(filePath,
                     false), autoFlush);
            }
            catch (FileNotFoundException e)
            {
               e.printStackTrace();
            }
            ln_counter = 0;
         }
      }

      public void close()
      {
         internalWriter.close();
      }

      public synchronized void writeRecord(String rec, boolean printStack, Throwable t)
      {
         long tid = Thread.currentThread().getId();
         println(LoggingUtil.getCurrentTime() + "-[t" + tid + "t] " + rec);

         if (printStack)
         {
            println("----------------Thread Stack Trace------------------");
            StackTraceElement[] traces = Thread.currentThread().getStackTrace();
            for (StackTraceElement e : traces)
            {
               println(e.toString());
            }
            println("----------------End Thread Stack Trace------------------");
         }

         if (t != null)
         {
            println("----------------Exception " + t + "------------------");
            StackTraceElement[] traces = t.getStackTrace();
            for (StackTraceElement e : traces)
            {
               println(e.toString());
            }
            println("----------------End Exception------------------");
         }
      }

   }

   static class LoggingUtil
   {
      private static SimpleDateFormat format = new SimpleDateFormat("MM:dd-HH:mm:ss:SSS");

      public static String getCurrentTime()
      {
         Date d = new Date();
         return format.format(d);
      }
   }

   public static void main(String[] args) throws IOException
   {
      DebugLogger logger = new DebugLogger("mylog");
      for (int i = 0; i < 23; i++)
      {
         logger.log("hello world!", false, null);
      }
      DebugLogger logger1 = new DebugLogger("mylog");
      for (int i = 0; i < 23; i++)
      {
         logger1.log("hello world 1 ! ", false, null);
      }
   }

}
