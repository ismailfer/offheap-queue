package com.ismail.queue.offheap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;

import com.ismail.queue.QueueAppender;
import com.ismail.queue.QueueTailer;
import com.ismail.util.StringUtil;

import sun.misc.Unsafe;

/**
 * OffHeapUnsafeQueue uses sun.misc.Unsafe to allocate memory offheap
 * 
 * Advantage of using Unsafe to allocate Memory; is that we can allocate more memory; basically the max is Long.MAX_VALUE; 
 * or max available RAM whichever is lower
 * 
 * On the otherside; if we use DirectByteBuffer; it can only allocate a max of Integer.MAX_VALUE
 * whereas if we use 
 * 
 * Purpose of this OffHeapQueue is to use off heap memory as a queue; 
 * for faster low latency messaging between publisher/subscriber threads
 * 
 * This is a training exercise for low latency stuff; and to use unsafe
 * 
 * This queue is ever growing; it is not a FIFO queue
 * 
 * This queue supports:
 * - One publisher
 * - Multipler consumers
 * 
 * @author ismail
 * @since 20221016
 */
public class OffHeapUnsafeQueue
{
    private Unsafe unsafe;

    private String name;

    private long capacity;


    /**
     * cyclic queue acts as a ring buffer; when it gets full; it starts writing from first position again
     * However; beware that cyclic queues poses a problem if the writer caught up to the reader
     * data will be corrupted when reading
     */
    private boolean cyclic = false;

    private long addressStart;

    // boundary of this offheap queue
    private long addressEnd;
    
    // Local stuff

    private boolean active = false;


    private long firstMsgPosition = 0;

    private long lastMsgPosition = 0;

    public OffHeapUnsafeQueue()
    {
        this(OffHeapUnsafeQueue.class.getSimpleName(), 1024, false);
    }
    

    public OffHeapUnsafeQueue(String name, long capacity, boolean cyclic)
    {

        this.name = name;
        this.cyclic = cyclic;
        this.capacity = capacity;

        if (capacity < 10)
            throw new IllegalArgumentException("Invalid value for capacity");

        
        try
        {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new RuntimeException("Unable to get unsafe: " + e.getMessage(), e);
        }

        try
        {
            this.addressStart = unsafe.allocateMemory(capacity);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new RuntimeException("Unable to allocate memory: " + e.getMessage(), e);
        }

        active = true;
        

        firstMsgPosition = addressStart;
        firstMsgPosition += Long.BYTES; // first msg position
        firstMsgPosition += Long.BYTES; // last msg position
        firstMsgPosition += Long.BYTES; // msg count
        firstMsgPosition += Long.BYTES; // cyclic count

        lastMsgPosition = firstMsgPosition;
        
        addressEnd = addressStart + capacity;
        
        // header data
        setFirstMsgPosition(firstMsgPosition);
        setLastMsgPosition(lastMsgPosition);
        setMsgCount(0);
        setCyclicCount(0);
    }

    public void setFirstMsgPosition(long v)
    {
        unsafe.putLong(addressStart, v);
    }

    public long getFirstMsgPosition()
    {
        return unsafe.getLong(addressStart);
    }

    public void setLastMsgPosition(long v)
    {
        unsafe.putLong(addressStart + Long.BYTES * 1, v);
    }

    public long getLastMsgPosition()
    {
        return unsafe.getLong(addressStart + Long.BYTES * 1);
    }

    private void setMsgCount(long v)
    {
        unsafe.putLong(addressStart + Long.BYTES * 2, v);
    }
    
    public long getMsgCount()
    {
        return unsafe.getLong(addressStart + Long.BYTES * 2);
    }

    public long getCyclicCount()
    {
        return unsafe.getLong(addressStart + Long.BYTES * 3);
    }
    
    private void setCyclicCount(long v)
    {
        unsafe.putLong(addressStart + Long.BYTES * 3, v);
    }
    
    public long getCapacity()
    {
        return capacity;
    }

    public void close()
    {
        freeMemory();
    }

    public void freeMemory()
    {
        unsafe.freeMemory(addressStart);
    }
   
    public OffHeapQueueAppender createAppender()
    {
        OffHeapQueueAppender app = new OffHeapQueueAppender();

        return app;
    }

    public OffHeapQueueTailer createTailer()
    {
        OffHeapQueueTailer tailer = new OffHeapQueueTailer();

        return tailer;
    }

    /**
     * Appender that writes messages to the queue
     * 
     * Only one appender should be used for this queue
     * 
     * First 8 bytes: message length in bytes
     * Next N bytes: message content
     */
    public class OffHeapQueueAppender implements QueueAppender
    {
        private boolean draft = true;

        private int msgLength = 0;

        private long lastFinishedWritePos = 0;
        
        private long currMsgStart = 0;

        private long currWritePos = 0;

        public OffHeapQueueAppender()
        {
            lastFinishedWritePos = firstMsgPosition;
            
            currWritePos = lastFinishedWritePos;

            draft = true;
            
            setMsgCount(0);
            setCyclicCount(0);
        }

        @Override
        public void start()
        {
            draft = true;
            
            // leave first long space for msg count
            currMsgStart = lastFinishedWritePos;
            
            // allow space for msg length
            currWritePos += Integer.BYTES;

            // allow space for next msg pointer
            currWritePos += Long.BYTES;

        }


        @Override
        public void writeMsgLength(int v)
        {
            unsafe.putInt(currMsgStart, v);
        }
        
        @Override
        public void writeMsgConsumed(boolean v)
        {
            
        }
        
        @Override
        public void writeNextMsgPosition(long v)
        {
            unsafe.putLong(currMsgStart + Integer.BYTES, v);
        }
        
        @Override
        public void writeByte(byte v)
        {
            if (currWritePos > addressEnd)
                throw new IllegalStateException("Reached EOF");
            
            unsafe.putByte(currWritePos, v);
            currWritePos += Byte.BYTES;
        }

        @Override
        public void writeChar(char v)
        {
            if (currWritePos > addressEnd)
                throw new IllegalStateException("Reached EOF");

            unsafe.putChar(currWritePos, v);
            currWritePos += Character.BYTES;
        }
        
        @Override
        public void writeLong(long v)
        {
            if (currWritePos > addressEnd)
                throw new IllegalStateException("Reached EOF");

            unsafe.putLong(currWritePos, v);
            currWritePos += Long.BYTES;
        }

        @Override
        public void writeInt(int v)
        {
            if (currWritePos > addressEnd)
                throw new IllegalStateException("Reached EOF");

            unsafe.putInt( currWritePos, v);
            currWritePos += Integer.BYTES;
        }

        @Override
        public void writeDouble(double v)
        {
            if (currWritePos > addressEnd)
                throw new IllegalStateException("Reached EOF");

            unsafe.putDouble(currWritePos, v);
            currWritePos += Double.BYTES;
        }

        @Override
        public void writeBoolean(boolean v)
        {
            if (currWritePos > addressEnd)
                throw new IllegalStateException("Reached EOF");

            byte bb = (byte) (v ? 1 : 0);
            unsafe.putByte(currWritePos, bb);
            currWritePos += Byte.BYTES;
        }

        @Override
        public void writeString(String v)
        {

            if (StringUtil.isDefined(v))
            {
                // write the char array
                byte[] bb = v.getBytes();

                if (currWritePos + Integer.BYTES + bb.length * Byte.BYTES > addressEnd)
                    throw new IllegalStateException("Reached EOF");

                // put size of the String first
                unsafe.putInt(currWritePos, bb.length);
                currWritePos += Integer.BYTES;

                for (int i = 0; i < bb.length; i++)
                {
                    unsafe.putByte(currWritePos, bb[i]);
                    currWritePos += Byte.BYTES;
                }
                
            }
            else
            {
                if (currWritePos > addressEnd)
                    throw new IllegalStateException("Reached EOF");

                // put size of the String first (0)
                unsafe.putInt(currWritePos, 0);
                currWritePos += Integer.BYTES;
            }
        }

        @Override
        public void writeObject(Serializable v)
        {
            // serialize the object

            if (v != null)
            {
                try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); //
                        ObjectOutputStream oos = new ObjectOutputStream(bos))
                {
                    oos.writeObject(v);

                    byte[] bb = bos.toByteArray();

                    if (currWritePos + Integer.BYTES + bb.length * Byte.BYTES >= addressEnd)
                        throw new IllegalStateException("Reached EOF");

                    // put size of the String first
                    unsafe.putInt(currWritePos, bb.length);
                    currWritePos += Integer.BYTES;

                    for (int i = 0; i < bb.length; i++)
                    {
                        unsafe.putByte(currWritePos, bb[i]);
                        currWritePos += Byte.BYTES;
                    }
                    
                    
                }
                catch (IOException io)
                {
                    io.printStackTrace();
                    
                    throw new RuntimeException("Unable to write object: " + io.getMessage(), io);
                }
            }
            // null value; 
            else
            {
                if (currWritePos >= addressEnd)
                    throw new IllegalStateException("Reached EOF");

                // put size of the String first (0)
                unsafe.putInt(currWritePos, 0);
                currWritePos += Integer.BYTES;
            }
        }

        
        @Override
        public void finish()
        {
            draft = false;

            msgLength = (int)(currWritePos - currMsgStart - Integer.BYTES);

            // Cyclic check: if the queue is cyclic; and we are nearing the end; 
            // then point to the beginning of the queue again
            if (cyclic)
            {
                // estimate next msg length; based on current msg length
                long estimatedNextMsgEndPosition = currWritePos + msgLength * 2;
                if (estimatedNextMsgEndPosition > addressEnd)
                {
                    long cyclicCount = getCyclicCount() + 1;
                    
                    setCyclicCount(cyclicCount);
                    
                    System.out.println("reached queue EOF! cycling the queue #" + cyclicCount);
                    
                    currWritePos = firstMsgPosition;
                }
            }
            
            // write message length
            writeMsgLength(msgLength);
            writeNextMsgPosition(currWritePos);

            long msgCount = getMsgCount() + 1;
            setMsgCount(msgCount);

            lastMsgPosition = currMsgStart;
            lastFinishedWritePos = currWritePos;
            
            setLastMsgPosition(lastMsgPosition);
        }
    }

    /**
     * Tailer that reads the message queue (aka iterator)
     * Many tailers can be used to read from this queue
     */
    public class OffHeapQueueTailer implements QueueTailer
    {
        private boolean readingMode = true;

        private long lastReadingPos = 0;
        
        private long currMsgStart = 0;

        private long currMsgLength = 0;
        
        private long nextMsgPosition = 0;
        
        private long currReadingPos = 0;

        private long msgReadCount = 0;

        public OffHeapQueueTailer()
        {
            currReadingPos = 0;
            msgReadCount = 0;
        }

        @Override
        public boolean nextMessage()
        {
            long queueMsgCount = getMsgCount();

            boolean newData = (msgReadCount < queueMsgCount);
            if (newData)
            {
                // if this is first record
                if (currReadingPos == 0)
                {
                    currMsgStart = getFirstMsgPosition();
                }
                else
                {
                    currMsgStart = lastReadingPos;
                }
                
                currReadingPos = currMsgStart;
                currMsgLength = readMsgLength();
                nextMsgPosition = readNextMsgPosition();
                
                readingMode = true;
            }

            return newData;
        }

        public int readMsgLength()
        {
           int v = unsafe.getInt(currReadingPos);
           currReadingPos += Integer.BYTES;
           
           return v;
        }
        
        private long readNextMsgPosition()
        {
            long v = unsafe.getLong(currReadingPos);
            currReadingPos += Long.BYTES;

            return v;
        }
        
        @Override
        public byte readByte()
        {
            byte v = unsafe.getByte(currReadingPos);
            currReadingPos += Byte.BYTES;

            return v;
        }

        @Override
        public char readChar()
        {
            char v = unsafe.getChar(currReadingPos);
            currReadingPos += Character.BYTES;

            return v;
        }

        @Override
        public long readLong()
        {
            long v = unsafe.getLong(currReadingPos);
            currReadingPos += Long.BYTES;

            return v;
        }

        @Override
        public int readInt()
        {
            int v = unsafe.getInt(currReadingPos);
            currReadingPos += Integer.BYTES;

            return v;
        }

        @Override
        public double readDouble()
        {
            double v = unsafe.getDouble(currReadingPos);
            currReadingPos += Double.BYTES;

            return v;
        }

        @Override
        public boolean readBoolean()
        {
            byte v = unsafe.getByte(currReadingPos);
            currReadingPos += Byte.BYTES;

            boolean bv = (v == (byte) 1);

            return bv;
        }

        @Override
        public String readString()
        {
            // get size of string
            int stringLen = unsafe.getInt(currReadingPos);
            currReadingPos += Integer.BYTES;

            String str = null;
            
            if (stringLen > 0)
            {
                byte[] bb = new byte[stringLen];
                for (int i=0; i<bb.length; i++)
                {
                    bb[i] = unsafe.getByte(currReadingPos);
                    currReadingPos += Byte.BYTES;
                }

                str = new String(bb);                
            }
            
            return str;
        }

        @Override
        public Object readObject()
        {
            // get size of the object
            int stringLen = unsafe.getInt(currReadingPos);
            currReadingPos += Integer.BYTES;

            if (stringLen > 0)
            {
                byte[] bb = new byte[stringLen];
                for (int i=0; i<bb.length; i++)
                {
                    bb[i] = unsafe.getByte(currReadingPos);
                    currReadingPos += Byte.BYTES;
                }

                Object v = null;

                try (ByteArrayInputStream bis = new ByteArrayInputStream(bb); //
                        ObjectInputStream in = new ObjectInputStream(bis))
                {
                    v = in.readObject();
                    
                    return v;
                }
                catch (IOException io)
                {
                    throw new RuntimeException("Unable to read object: " + io.getMessage(), io);
                }
                catch (ClassNotFoundException io)
                {
                    throw new RuntimeException("Unable to read object: " + io.getMessage(), io);
                }
            }
            else
            {
                return null;
            }
        }

        @Override
        public void finish()
        {
            readingMode = false;
            
            // update currReadingPos to the next msg pointer
            currReadingPos = nextMsgPosition;
            
            lastReadingPos = currReadingPos;
            
            msgReadCount++;
            
            currMsgLength = 0;
        }
    }
}
