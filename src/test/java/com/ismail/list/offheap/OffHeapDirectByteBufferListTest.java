package com.ismail.list.offheap;

import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.ismail.list.ListAppender;
import com.ismail.list.ListTailer;
import com.ismail.queue.offheap.Data;

/**
 * Tests the performance of ordinary JDK queues vs OffHeapMemoryQueue
 * 
 * Note: to perform a proper performance test; we need to warm up the JVM !!!!
 * 
 * @author ismail
 * @since 20221016
 */
public class OffHeapDirectByteBufferListTest
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
     * 
     */
    @Test
    public void testOffHeapQueueOneThread()
    {
        try
        {
            OffHeapDirectByteBufferList queue = new OffHeapDirectByteBufferList("ListTest1", iterations * 150 + 100);

            ListAppender app = queue.createAppender();
            ListTailer tailer = queue.createTailer();

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
                    "testOffHeapListOneThread: Written/Read " + (iterations) + " in " + (nanos / 1000000.0) + " ms, avg=" + (0.1 * nanos / iterations) + " nanos per msg");

            queue.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void testOffHeapList_ReadingAndSeeking()
    {
        try
        {
            OffHeapDirectByteBufferList list = new OffHeapDirectByteBufferList("ListTest1", iterations * 150 + 100);

            ListAppender app = list.createAppender();

            iterations = 10;
            int msgWrittenCount = 0;

            // -------------------------------------------------------------------------------
            // Writer Thread
            // -------------------------------------------------------------------------------
            // write few messages
            for (int i = 0; i < iterations; i++)
            {
                Instant inst = Instant.now();

                app.start();
                app.writeChar('M');
                app.writeLong(i);
                app.writeLong(inst.getEpochSecond());
                app.writeInt(inst.getNano());
                app.writeDouble(i);
                app.writeString("Hello");

                app.finish();

                msgWrittenCount++;
            }

            // -------------------------------------------------------------------------------
            // Reader Thread #1
            // -------------------------------------------------------------------------------
            {
                ListTailer tailer = list.createTailer();
                int msgReadCount = 0;

                // read the messages
                for (int i = 0; i < iterations; i++)
                {
                    if (tailer.nextMessage() == false)
                        break;

                    char msgType = tailer.readChar();
                    long index = tailer.readLong();
                    long time = tailer.readLong();
                    int nano = tailer.readInt();
                    double price = tailer.readDouble();
                    String notes = tailer.readString();

                    tailer.finish();

                    System.out.println("Reader1 <- " + index);

                    Assertions.assertTrue(msgType == 'M');
                    Assertions.assertTrue(index == i);
                    Assertions.assertTrue(price == i);
                    Assertions.assertTrue("Hello".equals(notes));

                    msgReadCount++;
                }

                Assertions.assertTrue(msgWrittenCount == msgReadCount);

            }

            // -------------------------------------------------------------------------------
            // Reader Thread #2 (reads later; gets last msg only)
            // -------------------------------------------------------------------------------
            {
                ListTailer tailer = list.createTailer();

                // read last msg only
                boolean hasLastMsg = tailer.seekLast();

                Assertions.assertTrue(hasLastMsg);

                if (hasLastMsg)
                {
                    char msgType = tailer.readChar();
                    long index = tailer.readLong();
                    long time = tailer.readLong();
                    int nano = tailer.readInt();
                    double price = tailer.readDouble();
                    String notes = tailer.readString();

                    tailer.finish();

                    System.out.println("Reader2 <- " + index);

                    Assertions.assertTrue(index == iterations - 1);

                }
            }

            // -------------------------------------------------------------------------------
            // Reader Thread #3 (reads first message only)
            // -------------------------------------------------------------------------------
            {
                ListTailer tailer = list.createTailer();

                // read last msg only
                boolean firstMsg = tailer.seekFirst();

                Assertions.assertTrue(firstMsg);

                if (firstMsg)
                {
                    char msgType = tailer.readChar();
                    long index = tailer.readLong();
                    long time = tailer.readLong();
                    int nano = tailer.readInt();
                    double price = tailer.readDouble();
                    String notes = tailer.readString();

                    tailer.finish();

                    System.out.println("Reader3 <- " + index);

                    Assertions.assertTrue(index == 0);

                }
            }

            // -------------------------------------------------------------------------------
            // Reader Thread #4 (reads message at a given index)
            // -------------------------------------------------------------------------------
            {
                ListTailer tailer = list.createTailer();

                long readIndex = 4;

                // read last msg only
                boolean msgAt = tailer.seekToIndex(readIndex);

                Assertions.assertTrue(msgAt);

                if (msgAt)
                {
                    char msgType = tailer.readChar();
                    long index = tailer.readLong();
                    long time = tailer.readLong();
                    int nano = tailer.readInt();
                    double price = tailer.readDouble();
                    String notes = tailer.readString();

                    tailer.finish();

                    System.out.println("Reader4 <- " + index);
                    
                    Assertions.assertTrue(index == readIndex);

                }

                // continue reading other messages
                while (tailer.nextMessage())
                {
                    readIndex++;

                    char msgType = tailer.readChar();
                    long index = tailer.readLong();
                    long time = tailer.readLong();
                    int nano = tailer.readInt();
                    double price = tailer.readDouble();
                    String notes = tailer.readString();

                    tailer.finish();

                    System.out.println("Reader4 <- " + index);
                    
                    Assertions.assertTrue(index == readIndex);


                }
            }

            list.close();
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
    public void testOffHeapQueueMultiThread()
    {
        try
        {
            OffHeapDirectByteBufferList queue = new OffHeapDirectByteBufferList("ListTest1", iterations * 150 + 100);

            ListAppender app = queue.createAppender();

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
                        ListTailer tailer = queue.createTailer();

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

            System.out.println("testOffHeapListMultiThread(" + threadCount + "): Written/Read " + (iterations) + " in " + (nanos / 1000000.0) + " ms, avg="
                    + (0.1 * nanos / iterations) + " nanos per msg");

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

            int capacity = 1024 * iterations;

            OffHeapDirectByteBufferList queue = new OffHeapDirectByteBufferList("ListTest1", capacity);

            ListAppender app = queue.createAppender();
            ListTailer tailer = queue.createTailer();

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
                    .println("testOffHeapListObjects: Written/Read " + (iterations) + " in " + (nanos / 1000000.0) + " ms, avg=" + (0.1 * nanos / iterations) + " nanos per msg");

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
    @Test
    public void testOffHeapQueuePublisherConsumerThreads()
    {
        try
        {
            OffHeapDirectByteBufferList queue = new OffHeapDirectByteBufferList("ListTest1", iterations * 150 + 100);

            long nanos = System.nanoTime();

            iterations = 5;
            
            int readerThreadCount = 2;

            // -----------------------------------------------------------------------------
            // Publisher
            // -----------------------------------------------------------------------------

            Thread writerThread = new Thread()
            {
                public void run()
                {
                    ListAppender app = queue.createAppender();

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
                        ListTailer tailer = queue.createTailer();

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
                            int latencySeconds = (int)(inst.getEpochSecond() - seconds);
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

            System.out.println("testOffHeapListMultiThread(" + readerThreadCount + "): Written/Read " + (iterations) + " in " + (nanos / 1000000.0) + " ms, avg="
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
        OffHeapDirectByteBufferListTest app = new OffHeapDirectByteBufferListTest();

        System.out.println("========= OffHeapDirectByteBufferListTest - WARM UP ====================");

        // warm up
        app.iterations = 10;

        app.testArrayList();
        app.testBlockingQueue();
        app.testOffHeapQueueOneThread();
        app.testOffHeapQueueMultiThread();
        //app.testOffHeapQueueObjects();

        System.out.println();
        System.out.println("========= OffHeapDirectByteBufferListTest - TEST    ===================="); // test up
        app.iterations = 1_000_000;

        app.testArrayList();
        app.testBlockingQueue();
        app.testOffHeapQueueOneThread();
        app.testOffHeapQueueMultiThread();
        //app.testOffHeapQueueObjects();
    }
}
