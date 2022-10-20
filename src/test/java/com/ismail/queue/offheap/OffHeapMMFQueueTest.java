package com.ismail.queue.offheap;

import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.ismail.queue.QueueAppender;
import com.ismail.queue.QueueTailer;

/**
 * 
 * @author ismail
 * @since 20221020
 */
public class OffHeapMMFQueueTest
{
    public int iterations = 1_000_000;

    @Test
    public void testArrayList()
    {
        try
        {

            ArrayList<Data> queue = new ArrayList<>(iterations);

            long nanos = System.nanoTime();

            int msgWrittenCount = 0;
            int msgReadCount = 0;

            // write few messages
            for (int i = 0; i < iterations; i++)
            {
                Data d = new Data();
                d.index = i;
                d.time = i;
                d.price = i;
                d.notes = "Hello";

                queue.add(d);

                msgWrittenCount++;
            }

            // read the messages
            for (int i = 0; i < iterations; i++)
            {
                Data d = queue.get(i);

                Assertions.assertTrue(d.msgType == 'M');
                Assertions.assertTrue(d.index == i);
                Assertions.assertTrue(d.time == i);
                Assertions.assertTrue(d.price == i);
                Assertions.assertTrue("Hello".equals(d.notes));

                msgReadCount++;
            }

            Assertions.assertTrue(msgWrittenCount == msgReadCount);

            nanos = System.nanoTime() - nanos;

            System.out.println("testArrayList      : Written/Read " + iterations + " in " + (nanos / 1000000.0) + " ms, avg=" + (0.1 * nanos / iterations) + " nanos per msg");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    /**
     * 
     */
    @Test
    public void testBlockingQueue()
    {
        try
        {
            BlockingQueue<Data> queue = new LinkedBlockingQueue<>(iterations);

            long nanos = System.nanoTime();

            int msgWrittenCount = 0;
            int msgReadCount = 0;

            // write few messages
            for (int i = 0; i < iterations; i++)
            {
                Data d = new Data();
                d.index = i;
                d.time = i;
                d.nanos = i;
                d.price = i;
                d.notes = "Hello";

                queue.put(d);

                msgWrittenCount++;
            }

            // read the messages
            for (int i = 0; i < iterations; i++)
            {
                Data d = queue.remove();

                Assertions.assertTrue(d.msgType == 'M');
                Assertions.assertTrue(d.index == i);
                Assertions.assertTrue(d.time == i);
                Assertions.assertTrue(d.price == i);
                Assertions.assertTrue("Hello".equals(d.notes));

                msgReadCount++;
            }

            Assertions.assertTrue(msgWrittenCount == msgReadCount);

            nanos = System.nanoTime() - nanos;

            System.out.println("testBlockingQueue  : Written/Read " + (iterations) + " in " + (nanos / 1000000.0) + " ms, avg=" + (0.1 * nanos / iterations) + " nanos per msg");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    /**
     * One thread writing
     * One thread reading
     */
    @Test
    public void testOffHeapMMFQueue()
    {
        try
        {
            String filePath = "queue.dat";

            // delete file if exists
            File file = new File(filePath);
            if (file.exists())
                file.delete();

            OffHeapMMFQueue<Data> queue = new OffHeapMMFQueue<>(filePath);

            long nanos = System.nanoTime();

            int msgWrittenCount = 0;
            int msgReadCount = 0;

            // write few messages
            for (int i = 0; i < iterations; i++)
            {
                Data d = new Data();
                d.index = i;
                d.time = i;
                d.nanos = i;
                d.price = i;
                d.notes = "Hello";

                queue.put(d);

                msgWrittenCount++;
            }

            // read the messages
            for (int i = 0; i < iterations; i++)
            {
                Data d = (Data) queue.get();

                Assertions.assertTrue(d != null);

                Assertions.assertTrue(d.msgType == 'M');
                Assertions.assertTrue(d.index == i);
                Assertions.assertTrue(d.time == i);
                Assertions.assertTrue(d.price == i);
                Assertions.assertTrue("Hello".equals(d.notes));

                msgReadCount++;
            }

            Assertions.assertTrue(msgWrittenCount == msgReadCount);

            nanos = System.nanoTime() - nanos;

            System.out.println("testOffHeapMMFQueue: Written/Read " + (iterations) + " in " + (nanos / 1000000.0) + " ms, avg=" + (0.1 * nanos / iterations) + " nanos per msg");

            queue.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void testOffHeapMMFQueue_MultiThreads()
    {
        try
        {
            String filePath = "queue.dat";

            // delete file if exists
            File file = new File(filePath);
            if (file.exists())
                file.delete();

            OffHeapMMFQueue<Data> queue = new OffHeapMMFQueue<>(filePath);

            long nanos = System.nanoTime();

            int iterations = 1000;

            int readerThreadCount = 2;

            int sleeTimeMillis = 0;
            int sleepTimeNanos = 0;

            boolean verbose = false;

            final AtomicInteger totalReadCount = new AtomicInteger(0);
            
            // -----------------------------------------------------------------------------
            // Publisher
            // -----------------------------------------------------------------------------

            Thread writerThread = new Thread()
            {
                public void run()
                {
                    int msgWrittenCount = 0;

                    // write few messages
                    for (int i = 0; i < iterations; i++)
                    {
                        Instant inst = Instant.now();

                        /* introduce latency for testing purposes
                        try
                        {
                            Thread.sleep(0, 1000);
                        }
                        catch (InterruptedException ie)
                        {
                        
                        }
                        */

                        if (verbose)
                            System.out.println(getName() + " -> msg #" + i);

                        Data d = new Data();
                        d.index = i;
                        d.time = inst.getEpochSecond();
                        d.nanos = inst.getNano();
                        d.price = i;
                        d.notes = "Hello";

                        queue.put(d);

                        msgWrittenCount++;

                        if (sleeTimeMillis > 0 && sleepTimeNanos > 0)
                        {
                            try
                            {
                                Thread.sleep(sleeTimeMillis, sleepTimeNanos);
                            }
                            catch (InterruptedException ie)
                            {

                            }
                        }
                    }
                }
            };
            writerThread.setDaemon(true);
            writerThread.setName("Publisher");

            // Start the publisher            
            writerThread.start();

            // -----------------------------------------------------------------------------
            // Consumer Threads
            // -----------------------------------------------------------------------------

            Thread[] readerThreads = new Thread[readerThreadCount];
            for (int t = 0; t < readerThreadCount; t++)
            {
                readerThreads[t] = new Thread()
                {
                    public void run()
                    {

                        int msgReadCount = 0;

                        // read the messages
                        while (true)
                        {
                            Data d = queue.get();

                            if (d == null)
                            {
                                if (totalReadCount.get() >= iterations)
                                    break;
                                
                                // busy wait
                                continue;
                            }

                            msgReadCount = totalReadCount.incrementAndGet();
                            
                            Assertions.assertTrue(d != null);

                            Assertions.assertTrue(d.msgType == 'M');
                            //Assertions.assertTrue(d.index == msgReadCount);
                            //Assertions.assertTrue(d.price == msgReadCount);
                            Assertions.assertTrue("Hello".equals(d.notes));
   
                            if (verbose)
                            {
                                Instant inst = Instant.now();

                                // calculate latency
                                int latencySeconds = (int) (inst.getEpochSecond() - d.time);
                                int latencyNanos = latencySeconds * 1000000000 + (inst.getNano() - d.nanos);

                                System.out.println(getName() + " <- msg #" + d.index + ", latency=" + latencyNanos + " ns");
                            }
                        }

                    }
                };
                readerThreads[t].setDaemon(true);
                readerThreads[t].setName("Consumer" + (t + 1) + "_of_" + readerThreadCount);
            }

            // Start the consumers
            for (int t = 0; t < readerThreadCount; t++)
                readerThreads[t].start();

            writerThread.join();

            for (int t = 0; t < readerThreadCount; t++)
                readerThreads[t].join();

            nanos = System.nanoTime() - nanos;

            System.out.println("testOffHeapMMFQueue_MultiThreads(" + readerThreadCount + "): Written/Read " + (iterations) + " in " + (nanos / 1000000.0) + " ms, avg="
                    + (0.1 * nanos / iterations) + " nanos per msg");

            // Make sure to close the queue
            queue.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static void main(String[] args)
    {
        OffHeapMMFQueueTest app = new OffHeapMMFQueueTest();

        System.out.println("========= OffHeapMMFQueueTest - WARM UP ====================");

        // warm up
        app.iterations = 10;

        app.testArrayList();
        app.testBlockingQueue();
        app.testOffHeapMMFQueue();
        app.testOffHeapMMFQueue_MultiThreads();

        System.out.println("========= OffHeapMMFQueueTest - TEST ====================");

        // test up
        app.iterations = 1_000;

        app.testArrayList();
        app.testBlockingQueue();
        app.testOffHeapMMFQueue();
        app.testOffHeapMMFQueue_MultiThreads();

    }
}
