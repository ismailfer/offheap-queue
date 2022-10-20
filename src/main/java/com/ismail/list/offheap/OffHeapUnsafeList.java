package com.ismail.list.offheap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;

import com.ismail.list.ListAppender;
import com.ismail.list.ListTailer;
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
public class OffHeapUnsafeList
{
    private Unsafe unsafe;

    private String name;

    private long capacity;

    private long addressStart;

    // boundary of this offheap queue
    private long addressEnd;

    // Local stuff

    private boolean active = false;

    private long msgCount = 0;

    private long firstMsgPosition = 0;

    private long lastMsgPosition = 0;

    public OffHeapUnsafeList(String name, long capacity)
    {
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

        this.name = name;

        this.capacity = capacity;

        if (capacity < 10)
            throw new IllegalArgumentException("Invalid value for capacity");

        try
        {
            this.addressStart = unsafe.allocateMemory(capacity);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new RuntimeException("Unable to allocate memory: " + e.getMessage(), e);
        }

        msgCount = 0;
        active = true;

        firstMsgPosition = addressStart + Long.BYTES + Long.BYTES + Long.BYTES;

        lastMsgPosition = firstMsgPosition;

        addressEnd = addressStart + capacity;

        // header data
        setFirstMsgPosition(firstMsgPosition);
        setLastMsgPosition(lastMsgPosition);
        setMsgCount(msgCount);
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

    public void setMsgCount(long v)
    {
        unsafe.putLong(addressStart + Long.BYTES * 2, v);
    }

    public long getMsgCount()
    {
        return unsafe.getLong(addressStart + Long.BYTES * 2);
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

    public OffHeapListAppender createAppender()
    {
        OffHeapListAppender app = new OffHeapListAppender();

        return app;
    }

    public OffHeapListTailer createTailer()
    {
        OffHeapListTailer tailer = new OffHeapListTailer();

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
    public class OffHeapListAppender implements ListAppender
    {
        private boolean draft = true;

        private int msgLength = 0;

        private long lastFinishedWritePos = 0;

        private long currMsgStart = 0;

        private long currWritePos = 0;

        public OffHeapListAppender()
        {
            lastFinishedWritePos = firstMsgPosition;

            currWritePos = lastFinishedWritePos;

            draft = true;
            msgCount = 0;
        }

        @Override
        public void start()
        {
            draft = true;

            // leave first long space for msg count
            currMsgStart = lastFinishedWritePos;

            // allow once long space for msg length
            currWritePos += Integer.BYTES;
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

            unsafe.putInt(currWritePos, v);
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
        public void writeMsgLength(int v)
        {
            unsafe.putInt(currMsgStart, v);
        }

        @Override
        public void finish()
        {
            draft = false;

            msgLength = (int) (currWritePos - currMsgStart - Integer.BYTES);

            // write message length
            writeMsgLength(msgLength);

            msgCount++;

            lastMsgPosition = currMsgStart;
            lastFinishedWritePos = currWritePos;

            setLastMsgPosition(lastMsgPosition);
            setMsgCount(msgCount);
        }
    }

    /**
     * Tailer that reads the message queue (aka iterator)
     * Many tailers can be used to read from this queue
     */
    public class OffHeapListTailer implements ListTailer
    {
        private boolean readingMode = true;

        private long lastCompletedReadingPos = 0;

        private long currMsgStart = 0;

        private long currMsgLength = 0;

        private long currReadingPos = 0;

        private long msgReadCount = 0;

        public OffHeapListTailer()
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
                    currMsgStart = lastCompletedReadingPos;
                }

                currReadingPos = currMsgStart;
                currMsgLength = readMsgLength();

                readingMode = true;
            }

            return newData;
        }

        @Override
        public boolean seekFirst()
        {
            long queueMsgCount = getMsgCount();

            boolean hasData = (queueMsgCount > 0);

            msgReadCount = 0;
            
            if (hasData)
            {
                currMsgStart = getFirstMsgPosition();
                currReadingPos = currMsgStart;
                currMsgLength = readMsgLength();

                readingMode = true;
            }

            return hasData;
        }

        @Override
        public boolean seekLast()
        {
            long queueMsgCount = getMsgCount();

            boolean hasData = (queueMsgCount > 0);

            if (hasData)
            {
                currMsgStart = getLastMsgPosition();

                currReadingPos = currMsgStart;
                currMsgLength = readMsgLength();

                readingMode = true;
                
                msgReadCount = queueMsgCount -1;
            }

            return hasData;
        }

        @Override
        public boolean seekToIndex(long index)
        {
            if (index < 0)
                throw new IndexOutOfBoundsException(index);

            long queueMsgCount = getMsgCount();

            if (index >= queueMsgCount)
                throw new IndexOutOfBoundsException(index);

            boolean hasData = (queueMsgCount > 0);

            if (hasData)
            {
                // get first msg
                currMsgStart = getFirstMsgPosition();
                currReadingPos = currMsgStart;
                currMsgLength = readMsgLength();
                
                // seek through the list till the requested index
                for (long l = 1; l <= index; l++)
                {
                    // add the msg length and another Integer byte for the msgLength field
                    currMsgStart += currMsgLength + Integer.BYTES;
                    
                    currReadingPos = currMsgStart;
                    currMsgLength = readMsgLength();
                }

                readingMode = true;
                
                msgReadCount = index;
            }

            return hasData;
        }

        @Override
        public int readMsgLength()
        {
            int v = unsafe.getInt(currReadingPos);
            currReadingPos += Integer.BYTES;

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
                for (int i = 0; i < bb.length; i++)
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
                for (int i = 0; i < bb.length; i++)
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

            // update currReadingPos (in case the reader did not read all fields
            currReadingPos = currMsgStart + currMsgLength + Integer.BYTES;
            lastCompletedReadingPos = currReadingPos;
            
            msgReadCount++;
            currMsgLength = 0;
        }
    }
}
