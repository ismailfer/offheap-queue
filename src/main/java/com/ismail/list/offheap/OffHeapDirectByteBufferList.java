package com.ismail.list.offheap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import com.ismail.list.ListAppender;
import com.ismail.list.ListTailer;
import com.ismail.util.StringUtil;

/**
 * Offheap Memory Queue using DirectByteBuffer to allocate memory offheap
 * 
 * Downside of DirectByteBuffer; is that it can only allocation a max of Integer.MAX_VALUE
 * whereas if we use Unsafe; we can allocation max of Long.MAX_VALUE; or max available RAM whichever is lower
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
public class OffHeapDirectByteBufferList
{
    private ByteBuffer buffer;

    private String name;

    private int capacity;

    // Local stuff

    private boolean active = false;

    private int msgCount = 0;

    private int firstMsgPosition = 0;

    private int lastMsgPosition = 0;

    public OffHeapDirectByteBufferList(String name, int capacity)
    {
        this.name = name;

        this.capacity = capacity;

        if (capacity < 10)
            throw new IllegalArgumentException("Invalid value for capacity");

        try
        {
            buffer = ByteBuffer.allocateDirect(capacity);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new RuntimeException("Unable to allocate DirectByteBuffer memory: " + e.getMessage(), e);
        }

        msgCount = 0;
        active = true;
        lastMsgPosition = 0;
        firstMsgPosition = 0 + Integer.BYTES + Integer.BYTES + Integer.BYTES;

        // header data
        setFirstMsgPosition(firstMsgPosition);
        setLastMsgPosition(lastMsgPosition);
        setMsgCount(msgCount);


    }

    public void setFirstMsgPosition(int v)
    {
        buffer.putInt(0, v);
    }

    public int getFirstMsgPosition()
    {
        return buffer.getInt(0);
    }

    public void setLastMsgPosition(int v)
    {
        buffer.putInt(0 + Integer.BYTES * 1, v);
    }

    public int getLastMsgPosition()
    {
        return buffer.getInt(0 + Integer.BYTES * 1);
    }

    public void setMsgCount(int v)
    {
        buffer.putInt(0 + Integer.BYTES * 2, v);
    }

    public int getMsgCount()
    {
        return buffer.getInt(0 + Integer.BYTES * 2);
    }

    public int getCapacity()
    {
        return capacity;
    }

    public void put(int i, byte value)
    {
        buffer.put(0 + i, value);
    }

    public byte get(int i)
    {
        return buffer.get(0 + i);
    }

    public void close()
    {

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

        private int lastFinishedWritePos = 0;

        private int currMsgStart = 0;

        private int currWritePos = 0;

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
            if (currWritePos >= capacity)
                throw new IllegalStateException("Reached EOF");

            buffer.put(currWritePos, v);
            currWritePos += Byte.BYTES;
        }

        @Override
        public void writeChar(char v)
        {
            if (currWritePos >= capacity)
                throw new IllegalStateException("Reached EOF");

            buffer.putChar(currWritePos, v);
            currWritePos += Character.BYTES;
        }

        @Override
        public void writeLong(long v)
        {
            if (currWritePos >= capacity)
                throw new IllegalStateException("Reached EOF");

            buffer.putLong(currWritePos, v);
            currWritePos += Long.BYTES;
        }

        @Override
        public void writeInt(int v)
        {
            if (currWritePos >= capacity)
                throw new IllegalStateException("Reached EOF");

            buffer.putInt(currWritePos, v);
            currWritePos += Integer.BYTES;
        }

        @Override
        public void writeDouble(double v)
        {
            if (currWritePos >= capacity)
                throw new IllegalStateException("Reached EOF");

            buffer.putDouble(currWritePos, v);
            currWritePos += Double.BYTES;
        }

        @Override
        public void writeBoolean(boolean v)
        {
            if (currWritePos >= capacity)
                throw new IllegalStateException("Reached EOF");

            byte bb = (byte) (v ? 1 : 0);
            buffer.put(currWritePos, bb);
            currWritePos += Byte.BYTES;
        }

        @Override
        public void writeString(String v)
        {
            if (StringUtil.isDefined(v))
            {
                // write the char array
                byte[] bb = v.getBytes();

                if (currWritePos + Integer.BYTES + bb.length * Byte.BYTES >= capacity)
                    throw new IllegalStateException("Reached EOF");

                // put size of the String first
                buffer.putInt(currWritePos, bb.length);

                currWritePos += Integer.BYTES;

                buffer.put(currWritePos, bb, 0, bb.length);
                currWritePos += bb.length * Byte.BYTES;

            }
            else
            {
                if (currWritePos >= capacity)
                    throw new IllegalStateException("Reached EOF");

                // put size of the String first (0)
                buffer.putInt(currWritePos, 0);
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

                    if (currWritePos + Integer.BYTES + bb.length * Byte.BYTES >= capacity)
                        throw new IllegalStateException("Reached EOF");

                    // put size of the String first
                    buffer.putInt(currWritePos, bb.length);
                    currWritePos += Integer.BYTES;

                    buffer.put(currWritePos, bb, 0, bb.length);
                    currWritePos += bb.length * Byte.BYTES;
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
                if (currWritePos >= capacity)
                    throw new IllegalStateException("Reached EOF");

                // put size of the String first (0)
                buffer.putInt(currWritePos, 0);
                currWritePos += Integer.BYTES;
            }
        }

        @Override
        public void writeMsgLength(int v)
        {
            buffer.putInt(currMsgStart, v);
        }

        @Override
        public void finish()
        {
            draft = false;

            msgLength = currWritePos - currMsgStart - Integer.BYTES;

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

        private int lastReadingPos = 0;

        private int currMsgStart = 0;

        private int currMsgLength = 0;

        private int currReadingPos = 0;

        private long msgReadCount = 0;

        public OffHeapListTailer()
        {
            currReadingPos = 0;
            msgReadCount = 0;
        }

        @Override
        public boolean nextMessage()
        {
            int queueMsgCount = getMsgCount();

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

                readingMode = true;
            }

            return newData;
        }

        @Override
        public boolean seekFirst()
        {
            long queueMsgCount = getMsgCount();

            boolean hasData = (queueMsgCount > 0);

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
            int v = buffer.getInt(currReadingPos);
            currReadingPos += Integer.BYTES;

            return v;
        }

        @Override
        public byte readByte()
        {
            byte v = buffer.get(currReadingPos);
            currReadingPos += Byte.BYTES;

            return v;
        }

        @Override
        public char readChar()
        {
            char v = buffer.getChar(currReadingPos);
            currReadingPos += Character.BYTES;

            return v;
        }

        @Override
        public long readLong()
        {
            long v = buffer.getLong(currReadingPos);
            currReadingPos += Long.BYTES;

            return v;
        }

        @Override
        public int readInt()
        {
            int v = buffer.getInt(currReadingPos);
            currReadingPos += Integer.BYTES;

            return v;
        }

        @Override
        public double readDouble()
        {
            double v = buffer.getDouble(currReadingPos);
            currReadingPos += Double.BYTES;

            return v;
        }

        @Override
        public boolean readBoolean()
        {
            byte v = buffer.get(currReadingPos);

            currReadingPos += Byte.BYTES;

            boolean bv = (v == (byte) 1);

            return bv;
        }

        @Override
        public String readString()
        {
            // get size of string
            int stringLen = buffer.getInt(currReadingPos);
            currReadingPos += Integer.BYTES;

            if (stringLen > 0)
            {
                byte[] bb = new byte[stringLen];

                buffer.get(currReadingPos, bb, 0, stringLen);
                currReadingPos += stringLen * Byte.BYTES;

                String str = new String(bb);

                return str;
            }
            else
            {
                return null;
            }
        }

        @Override
        public Object readObject()
        {
            // get size of the object
            int stringLen = buffer.getInt(currReadingPos);
            currReadingPos += Integer.BYTES;

            if (stringLen > 0)
            {
                byte[] bb = new byte[stringLen];

                buffer.get(currReadingPos, bb, 0, stringLen);
                currReadingPos += stringLen * Byte.BYTES;

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
            lastReadingPos = currReadingPos;
            
            msgReadCount++;
            currMsgLength = 0;
        }
    }
}
