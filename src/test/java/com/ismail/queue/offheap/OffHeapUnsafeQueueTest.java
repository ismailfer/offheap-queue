package com.ismail.queue.offheap;

import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.ismail.queue.QueueAppender;
import com.ismail.queue.QueueTailer;

/**
 * Tests the performance of ordinary JDK queues vs OffHeapMemoryQueue
 * 
 * @author ismail
 * @since 20221016
 */
public class OffHeapUnsafeQueueTest
{
    public int iterations = 1_000_000;

    /**
     * 
     */
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

            System.out.println("testArrayList Written/Read " + iterations + " in " + (nanos / 1000000.0) + " ms, avg=" + (0.1 * nanos / iterations) + " nanos per msg");
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

            System.out.println("testBlockingQueue: Written/Read " + (iterations) + " in " + (nanos / 1000000.0) + " ms, avg=" + (0.1 * nanos / iterations) + " nanos per msg");
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
    public void testOffHeapQueue()
    {
        try
        {
            OffHeapUnsafeQueue queue = new OffHeapUnsafeQueue("QueueTest1", iterations * 150L + 100, false);

            QueueAppender app = queue.createAppender();
            QueueTailer tailer = queue.createTailer();

            long nanos = System.nanoTime();

            int msgWrittenCount = 0;
            int msgReadCount = 0;

            // write few messages
            for (int i = 0; i < iterations; i++)
            {
                app.start();
                app.writeChar('M');
                app.writeInt(i);
                app.writeLong(i);
                app.writeDouble(i);
                app.writeString("Hello");

                app.finish();

                msgWrittenCount++;
            }

            // read the messages
            for (int i = 0; i < iterations; i++)
            {
                if (tailer.nextMessage() == false)
                    break;

                Assertions.assertTrue(tailer.readChar() == 'M');
                Assertions.assertTrue(tailer.readInt() == i);
                Assertions.assertTrue(tailer.readLong() == i);
                Assertions.assertTrue(tailer.readDouble() == i);
                Assertions.assertTrue("Hello".equals(tailer.readString()));

                tailer.finish();

                msgReadCount++;
            }

            Assertions.assertTrue(msgWrittenCount == msgReadCount);

            nanos = System.nanoTime() - nanos;

            System.out.println(
                    "testOffHeapQueue    : Written/Read " + (iterations) + " in " + (nanos / 1000000.0) + " ms, avg=" + (0.1 * nanos / iterations) + " nanos per msg");

            queue.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    /**
     * One thread writing
     * Multi therads reading
     */
    @Test
    public void testOffHeapQueue_MultiThread()
    {
        try
        {
            OffHeapUnsafeQueue queue = new OffHeapUnsafeQueue("QueueTest1", iterations * 150L + 100, false);

            QueueAppender app = queue.createAppender();

            long nanos = System.nanoTime();

            int msgWrittenCount = 0;

            // write few messages
            for (int i = 0; i < iterations; i++)
            {
                app.start();
                app.writeChar('M');
                app.writeInt(i);
                app.writeLong(i);
                app.writeDouble(i);
                app.writeString("Hello");

                app.finish();

                msgWrittenCount++;
            }

            final int msgWrittenCount2 = msgWrittenCount;

            int threadCount = 2;

            Thread[] tt = new Thread[threadCount];
            for (int t = 0; t < threadCount; t++)
            {
                tt[t] = new Thread()
                {
                    public void run()
                    {
                        QueueTailer tailer = queue.createTailer();

                        int msgReadCount = 0;

                        // read the messages
                        for (int i = 0; i < iterations; i++)
                        {
                            if (tailer.nextMessage() == false)
                                break;

                            Assertions.assertTrue(tailer.readChar() == 'M');
                            Assertions.assertTrue(tailer.readInt() == i);
                            Assertions.assertTrue(tailer.readLong() == i);
                            Assertions.assertTrue(tailer.readDouble() == i);
                            Assertions.assertTrue("Hello".equals(tailer.readString()));

                            tailer.finish();

                            msgReadCount++;
                        }

                        Assertions.assertTrue(msgWrittenCount2 == msgReadCount);
                    }
                };
            }

            for (int t = 0; t < threadCount; t++)
                tt[t].start();

            for (int t = 0; t < threadCount; t++)
                tt[t].join();

            nanos = System.nanoTime() - nanos;

            System.out.println("testOffHeapQueueMultiThread(" + threadCount + "): Written/Read " + (iterations) + " in " + (nanos / 1000000.0) + " ms, avg="
                    + (0.1 * nanos / iterations) + " nanos per msg");

            queue.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Tests the latency of receiving messages between the publisher and consumer
     * 
     * One thread writing
     * Multi threads reading
     * 
     * Note; the reading threads are doing busy waits; so the more threads you create; the higher the CPU usage!
     */
    public void testBlockingQueuePublisherConsumerThreads()
    {
        System.out.println("===== testBlockingQueuePublisherConsumerThreads =====");

        try
        {
            BlockingQueue<Data> queue = new LinkedBlockingQueue<>(iterations);

            long nanos = System.nanoTime();

            iterations = 5;

            int readerThreadCount = 1;

            // -----------------------------------------------------------------------------
            // Publisher
            // -----------------------------------------------------------------------------

            Thread writerThread = new Thread()
            {
                public void run()
                {

                    int msgWrittenCount = 0;

                    // write few messages
                    for (int i = 1; i <= iterations; i++)
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

                        System.out.println(getName() + " -> msg #" + i);

                        Data d = new Data();
                        d.index = i;
                        d.time = inst.getEpochSecond();
                        d.nanos = inst.getNano();
                        d.price = i;
                        d.notes = "Hello";

                        queue.add(d);

                        msgWrittenCount++;

                        try
                        {
                            Thread.sleep(1000);
                        }
                        catch (InterruptedException ie)
                        {

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
                            while (queue.isEmpty())
                            {
                                // busy loop
                            }

                            Data d = queue.remove();

                            Instant inst = Instant.now();

                            int index = d.index;
                            long seconds = d.time;
                            int nanos = d.nanos;

                            msgReadCount++;

                            // calculate latency
                            int latencySeconds = (int) (inst.getEpochSecond() - seconds);
                            int latencyNanos = latencySeconds * 1000000000 + (inst.getNano() - nanos);

                            System.out.println(getName() + " <- msg #" + index + ", latency=" + latencyNanos + " ns");

                            if (msgReadCount >= iterations)
                                break;
                        }

                    }
                };
                readerThreads[t].setDaemon(true);
                readerThreads[t].setName("Consumer" + (t + 1) + "_of_" + readerThreadCount);
            }

            // Start the consumers
            for (int t = 0; t < readerThreadCount; t++)
                readerThreads[t].start();

            for (int t = 0; t < readerThreadCount; t++)
                readerThreads[t].join();

            nanos = System.nanoTime() - nanos;

            System.out.println("testBlockingQueue_MultiThread(" + readerThreadCount + "): Written/Read " + (iterations) + " in " + (nanos / 1000000.0) + " ms, avg="
                    + (0.1 * nanos / iterations) + " nanos per msg");

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Tests the latency of receiving messages between the publisher and consumer
     * 
     * One thread writing
     * Multi threads reading
     * 
     * Note; the reading threads are doing busy waits; so the more threads you create; the higher the CPU usage!
     */
    public void testOffHeapQueuePublisherConsumerThreads()
    {
        System.out.println("===== testOffHeapQueuePublisherConsumerThreads =====");

        try
        {
            OffHeapUnsafeQueue queue = new OffHeapUnsafeQueue("QueueTest1", iterations * 150L + 100, false);

            long nanos = System.nanoTime();

            iterations = 5;

            int readerThreadCount = 1;

            // -----------------------------------------------------------------------------
            // Publisher
            // -----------------------------------------------------------------------------

            Thread writerThread = new Thread()
            {
                public void run()
                {
                    QueueAppender app = queue.createAppender();

                    int msgWrittenCount = 0;

                    // write few messages
                    for (int i = 1; i <= iterations; i++)
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

                        System.out.println(getName() + " -> msg #" + i);

                        app.start();
                        app.writeChar('M');
                        app.writeInt(i);

                        app.writeLong(inst.getEpochSecond());
                        app.writeInt(inst.getNano());

                        app.writeDouble(i);
                        app.writeString("Hello");

                        app.finish();

                        msgWrittenCount++;

                        try
                        {
                            Thread.sleep(1000);
                        }
                        catch (InterruptedException ie)
                        {

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
                        QueueTailer tailer = queue.createTailer();

                        int msgReadCount = 0;

                        // read the messages
                        while (true)
                        {
                            while (tailer.nextMessage() == false)
                            {
                                // busy wait
                            }

                            Instant inst = Instant.now();

                            char msgType = tailer.readChar();
                            int index = tailer.readInt();

                            long seconds = tailer.readLong();
                            int nanos = tailer.readInt();

                            double price = tailer.readDouble();
                            String notes = tailer.readString();

                            tailer.finish();

                            msgReadCount++;

                            // calculate latency
                            int latencySeconds = (int) (inst.getEpochSecond() - seconds);
                            int latencyNanos = latencySeconds * 1000000000 + (inst.getNano() - nanos);

                            System.out.println(getName() + " <- msg #" + index + ", latency=" + latencyNanos + " ns");

                            if (msgReadCount >= iterations)
                                break;
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

            System.out.println("testOffHeapQueueMultiThread(" + readerThreadCount + "): Written/Read " + (iterations) + " in " + (nanos / 1000000.0) + " ms, avg="
                    + (0.1 * nanos / iterations) + " nanos per msg");

            // Make sure to close the queue
            queue.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Test writing/reading objects
     */
    @Test
    public void testOffHeapQueueObjects()
    {
        try
        {
            //iterations = 10;

            OffHeapUnsafeQueue queue = new OffHeapUnsafeQueue("QueueTest1", iterations * 2000 + 100, false);

            QueueAppender app = queue.createAppender();
            QueueTailer tailer = queue.createTailer();

            long nanos = System.nanoTime();

            int msgWrittenCount = 0;
            int msgReadCount = 0;

            // write few messages
            for (int i = 0; i < iterations; i++)
            {
                app.start();

                Data d = new Data();
                d.index = i;
                d.time = i;
                d.price = i;
                d.notes = "Hello";

                app.writeObject(d);

                app.finish();

                msgWrittenCount++;
            }

            // read the messages
            for (int i = 0; i < iterations; i++)
            {
                if (tailer.nextMessage() == false)
                    break;

                Object obj = tailer.readObject();

                Data d = (Data) obj;

                Assertions.assertTrue(obj != null);
                Assertions.assertTrue(d.index == i);
                Assertions.assertTrue(d.time == i);
                Assertions.assertTrue(d.price == i);
                Assertions.assertTrue("Hello".equals(d.notes));

                tailer.finish();

                msgReadCount++;
            }

            Assertions.assertTrue(msgWrittenCount == msgReadCount);

            nanos = System.nanoTime() - nanos;

            System.out
                    .println("testOffHeapQueueObjects: Written/Read " + (iterations) + " in " + (nanos / 1000000.0) + " ms, avg=" + (0.1 * nanos / iterations) + " nanos per msg");

            queue.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void testOffHeapQueue_Cyclic()
    {
        try
        {
            iterations = 1000;

            OffHeapUnsafeQueue queue = new OffHeapUnsafeQueue("CyclicQueue", 5000, true);

            QueueAppender app = queue.createAppender();
            QueueTailer tailer = queue.createTailer();

            long nanos = System.nanoTime();

            int msgWrittenCount = 0;
            int msgReadCount = 0;

            // write few messages
            for (int i = 0; i < iterations; i++)
            {
                app.start();
                app.writeChar('M');
                app.writeInt(i);
                app.writeLong(i);
                app.writeDouble(i);
                app.writeString("Hello");

                app.finish();

                msgWrittenCount++;
            }

            /*
            // read the messages
            for (int i = 0; i < iterations; i++)
            {
                if (tailer.nextMessage() == false)
                    break;
            
                Assertions.assertTrue(tailer.readChar() == 'M');
                Assertions.assertTrue(tailer.readInt() == i);
                Assertions.assertTrue(tailer.readLong() == i);
                Assertions.assertTrue(tailer.readDouble() == i);
                Assertions.assertTrue("Hello".equals(tailer.readString()));
            
                tailer.finish();
            
                msgReadCount++;
            }
            
            Assertions.assertTrue(msgWrittenCount == msgReadCount);
            */

            nanos = System.nanoTime() - nanos;

            System.out
                    .println("testOffHeapQueue_Cyclic: Written/Read " + (iterations) + " in " + (nanos / 1000000.0) + " ms, avg=" + (0.1 * nanos / iterations) + " nanos per msg");

            queue.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void testOffHeapQueue_Cyclic_Producer_Consumer()
    {
        System.out.println("===== testOffHeapQueue_Cyclic_Producer_Consumer =====");

        try
        {
            long iterations = 1000;

            int readerThreadCount = 1;

            int publishSleepTimeMillis = 0;
            int publishSleepTimeNano = 1;

            boolean verbose = false;

            OffHeapUnsafeQueue queue = new OffHeapUnsafeQueue("CyclicQueue", 1000, true);

            long nanos = System.nanoTime();

            // -----------------------------------------------------------------------------
            // Publisher
            // -----------------------------------------------------------------------------

            Thread writerThread = new Thread()
            {
                public void run()
                {
                    System.out.println(getName() + " started");

                    QueueAppender app = queue.createAppender();

                    long msgWrittenCount = 0;

                    // write few messages
                    for (long i = 0; i < iterations; i++)
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

                        app.start();
                        app.writeChar('M');
                        app.writeLong(i);

                        app.writeLong(inst.getEpochSecond());
                        app.writeInt(inst.getNano());

                        app.writeDouble(i);
                        app.writeString("Hello");

                        app.finish();

                        msgWrittenCount++;

                        if (publishSleepTimeMillis > 0 || publishSleepTimeNano > 0)
                        {
                            try
                            {
                                Thread.sleep(publishSleepTimeMillis, publishSleepTimeNano);
                            }
                            catch (InterruptedException ie)
                            {
                                ie.printStackTrace();
                            }
                        }
                    }

                    System.out.println(getName() + " exited");

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
                        System.out.println(getName() + " started");

                        QueueTailer tailer = queue.createTailer();

                        long msgReadCount = 0;

                        // read the messages
                        while (true)
                        {
                            while (tailer.nextMessage() == false)
                            {
                                // busy wait
                            }

                            Instant inst = Instant.now();

                            char msgType = tailer.readChar();
                            long index = tailer.readLong();

                            long seconds = tailer.readLong();
                            int nanos = tailer.readInt();

                            double price = tailer.readDouble();
                            String notes = tailer.readString();

                            tailer.finish();

                            // verify that we got the right message
                            if (index != msgReadCount)
                            {
                                System.err.println(getName() + " <- msg #" + index + " != readCount #" + msgReadCount + ", missed " + (index-msgReadCount));
                                
                            }
                            else
                            {
                                if (verbose)
                                {
                                    // calculate latency
                                    int latencySeconds = (int) (inst.getEpochSecond() - seconds);
                                    int latencyNanos = latencySeconds * 1000000000 + (inst.getNano() - nanos);

                                    System.out.println(getName() + " <- msg #" + index + ", latency=" + latencyNanos + " ns");
                                }
                            }
                            //Assertions.assertTrue(index == msgReadCount);
                            
                            msgReadCount++;

  

                            if (msgReadCount >= iterations)
                                break;
                        }

                        System.out.println(getName() + " exited");

                    }
                };
                readerThreads[t].setDaemon(true);
                readerThreads[t].setName("Consumer" + (t + 1) + "_of_" + readerThreadCount);
            }

            // Start the consumers
            for (int t = 0; t < readerThreadCount; t++)
                readerThreads[t].start();

            for (int t = 0; t < readerThreadCount; t++)
                readerThreads[t].join(10000L);

            nanos = System.nanoTime() - nanos;

            System.out.println("testOffHeapQueue_Cyclic_Producer_Consumer(" + readerThreadCount + "): cyclicCount=" + queue.getCyclicCount() + ", Written/Read " + (iterations)
                    + " in " + (nanos / 1000000.0) + " ms, avg=" + (0.1 * nanos / iterations) + " nanos per msg");

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
        OffHeapUnsafeQueueTest app = new OffHeapUnsafeQueueTest();

        System.out.println("========= OffHeapUnsafeQueueTest - WARM UP ====================");

        // warm up
        app.iterations = 10;

        app.testArrayList();
        app.testBlockingQueue();
        app.testOffHeapQueue();
        app.testOffHeapQueue_MultiThread();
        //app.testOffHeapQueueObjects();

        System.out.println("========= OffHeapUnsafeQueueTest - TEST ====================");

        // test up
        app.iterations = 1_000_000;

        app.testArrayList();
        app.testBlockingQueue();
        app.testOffHeapQueue();
        app.testOffHeapQueue_MultiThread();
        //app.testOffHeapQueueObjects();
    }
}
