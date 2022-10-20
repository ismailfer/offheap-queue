package com.ismail.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.ismail.queue.offheap.Data;
import com.ismail.util.serialization.IssySerializer;
import com.ismail.util.serialization.SerializationUtil;

/**
 * @author ismail
 * @since 20221018
 */
public class SerializationTest
{
   public int iterations = 10_000;
    
    @Test
    public void testSerializationJDK()
    {
        try
        {
            //int iterations = 1_000_000;

            long nanos = System.nanoTime();

            // write few messages
            for (int i = 0; i < iterations; i++)
            {
                Data d = new Data();
                d.index = i;
                d.time = i;
                d.price = i;
                d.notes = "Hello";

                byte[] bb = SerializationUtil.serializeUsingJDK(d);

                Data d2 = (Data) SerializationUtil.deserializeUsingJDK(bb);

                Assertions.assertTrue(d.index == d2.index);
            }

            nanos = System.nanoTime() - nanos;

            System.out
                    .println("testSerializationJDK       : Written/Read " + (iterations) + " in " + (nanos / 1000000.0) + " ms, avg=" + (0.1 * nanos / iterations) + " nanos per msg");

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testSerializationApacheLang()
    {
        try
        {
            //int iterations = 1_000_000;

            long nanos = System.nanoTime();

            // write few messages
            for (int i = 0; i < iterations; i++)
            {
                Data d = new Data();
                d.index = i;
                d.time = i;
                d.price = i;
                d.notes = "Hello";

                byte[] bb = SerializationUtil.serializeUsingApacheLang3(d);

                Data d2 = (Data) SerializationUtil.deserializeUsingApacheLang3(bb);

                Assertions.assertTrue(d.index == d2.index);
            }

            nanos = System.nanoTime() - nanos;

            System.out
                    .println("testSerializationApacheLang: Written/Read " + (iterations) + " in " + (nanos / 1000000.0) + " ms, avg=" + (0.1 * nanos / iterations) + " nanos per msg");

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testSerializationIssy()
    {
        try
        {
            //int iterations = 1_000_000;

            long nanos = System.nanoTime();

            // write few messages
            for (int i = 0; i < iterations; i++)
            {
                Data d = new Data();
                d.index = i;
                d.time = i;
                d.price = i;
                d.notes = "Hello";

                byte[] bb = SerializationUtil.serializeUsingIssy(d);

                Data d2 = (Data) SerializationUtil.deserializeUsingIssy(bb);

                Assertions.assertTrue(d.index == d2.index);
            }

            nanos = System.nanoTime() - nanos;

            System.out
                    .println("testSerializationIssy      : Written/Read " + (iterations) + " in " + (nanos / 1000000.0) + " ms, avg=" + (0.1 * nanos / iterations) + " nanos per msg");

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testSerializationIssyFast1()
    {
        try
        {
            //iterations = 10;

            long nanos = System.nanoTime();

            IssySerializer serializer = new IssySerializer(1024);
            
            // write few messages
            for (int i = 0; i < iterations; i++)
            {
                Data d = new Data();
                d.index = i;
                d.time = i;
                d.price = i;
                d.notes = "Hello";

                byte[] bb = serializer.serialize(d);

                Data d2 = (Data) serializer.deserialize(bb);

                boolean equals1 = (d.index == d2.index);
                
                boolean equals2 = (d.time == d2.time);
                
                Assertions.assertTrue(d.index == d2.index);
            }

            nanos = System.nanoTime() - nanos;

            System.out
                    .println("testSerializationIssyFast1 : Written/Read " + (iterations) + " in " + (nanos / 1000000.0) + " ms, avg=" + (0.1 * nanos / iterations) + " nanos per msg");

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testSerializationIssyFast2()
    {
        try
        {
            //iterations = 10;

            long nanos = System.nanoTime();

            IssySerializer serializer = new IssySerializer(1024);
            
            byte[] bb = new byte[10000];
            
            // write few messages
            for (int i = 0; i < iterations; i++)
            {
                Data d = new Data();
                d.index = i;
                d.time = i;
                d.price = i;
                d.notes = "Hello";

                int size = serializer.serializeIntoBuff(d, bb, 0);

                Data d2 = (Data) serializer.deserializeFromByteRange(bb, 0, size);

                boolean equals1 = (d.index == d2.index);
                
                boolean equals2 = (d.time == d2.time);
                
                Assertions.assertTrue(d.index == d2.index);
            }

            nanos = System.nanoTime() - nanos;

            System.out
                    .println("testSerializationIssyFast2 : Written/Read " + (iterations) + " in " + (nanos / 1000000.0) + " ms, avg=" + (0.1 * nanos / iterations) + " nanos per msg");

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
    
    
    public static void main(String[] args)
    {
        SerializationTest app = new SerializationTest();

        System.out.println("==================== WARM UP ====================");

        // warm up
        app.iterations = 10;
        
        app.testSerializationJDK();
        app.testSerializationApacheLang();
        app.testSerializationIssy();
        app.testSerializationIssyFast1();
        app.testSerializationIssyFast2();
        
        System.out.println("==================== TEST   ====================");
        // test up
        app.iterations = 1_000_000;
        
        app.testSerializationJDK();
        app.testSerializationApacheLang();
        app.testSerializationIssy();
        app.testSerializationIssyFast1();
        app.testSerializationIssyFast2();
    }
}
