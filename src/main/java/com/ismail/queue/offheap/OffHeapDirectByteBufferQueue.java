package com.ismail.queue.offheap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import com.ismail.queue.QueueAppender;
import com.ismail.queue.QueueTailer;
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
public class OffHeapDirectByteBufferQueue
{
    private ByteBuffer buffer;

    private String name;

    private int capacity;

    /**
     * cyclic queue acts as a ring buffer; when it gets full; it starts writing from first position again
     * However; beware that cyclic queues poses a problem if the writer caught up to the reader
     * data will be corrupted when reading
     */
    private boolean cyclic = false;

    // Local stuff

    private boolean active = false;


    //private int msgCount = 0;

    private int firstMsgPosition = 0;

    private int lastMsgPosition = 0;

    private int queueEndPos = 0;

    public OffHeapDirectByteBufferQueue()
    {
        this(OffHeapDirectByteBufferQueue.class.getSimpleName(), 1024, false);
    }

    /**
     * @param name
     * @param capacity
     * @param cyclic queue acts as a ring buffer; when it gets full; it starts writing from first position again
     */
    public OffHeapDirectByteBufferQueue(String name, int capacity, boolean cyclic)
    {
        this.name = name;
        this.capacity = capacity;
        this.cyclic = cyclic;

        // input validation
        if (capacity < 10)
            throw new IllegalArgumentException("Invalid value for capacity");

        // memory allocation
        try
        {
            buffer = ByteBuffer.allocateDirect(capacity);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new RuntimeException("Unable to allocate DirectByteBuffer memory: " + e.getMessage(), e);
        }
        
        active = true;

        lastMsgPosition = 0;

        firstMsgPosition = 0;
        firstMsgPosition += Integer.BYTES; // first msg position
        firstMsgPosition += Integer.BYTES; // last msg position
        firstMsgPosition += Long.BYTES; // msg count
        firstMsgPosition += Long.BYTES; // cyclic count

        queueEndPos = capacity;

        // header data
        setFirstMsgPosition(firstMsgPosition);
        setLastMsgPosition(lastMsgPosition);
        setMsgCount(0);
        setCyclicCount(0);
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

    private void setMsgCount(long v)
    {
        buffer.putLong(0 + Integer.BYTES * 2, v);
    }

    public long getMsgCount()
    {
        return buffer.getLong(0 + Integer.BYTES * 2);
    }

    /**
     * Indicates how many times the queue got full; and restarted from the beginning
     * Only applicable if cyclic=true;
     */
    public long getCyclicCount()
    {
        return buffer.getLong(0 + Integer.BYTES * 2 + Long.BYTES);
    }

    private void setCyclicCount(long v)
    {
        buffer.putLong(0 + Integer.BYTES * 2 + Long.BYTES, v);
    }

    public int getCapacity()
    {
        return capacity;
    }

    public void close()
    {

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

        private int lastFinishedWritePos = 0;

        private int currMsgStart = 0;

        private int currWritePos = 0;

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

            // allow space for pointer to next message
            currWritePos += Integer.BYTES;

        }

        @Override
        public void writeMsgLength(int v)
        {
            buffer.putInt(currMsgStart, v);
        }

        @Override
        public void writeMsgConsumed(boolean v)
        {
            
        }
        
        @Override
        public void writeNextMsgPosition(long v)
        {
            buffer.putInt(currMsgStart + Integer.BYTES, (int) v);
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
        public void finish()
        {
            draft = false;

            msgLength = (int) (currWritePos - currMsgStart - Integer.BYTES - Integer.BYTES);

            // Cyclic check: if the queue is cyclic; and we are nearing the end; 
            // then point to the beginning of the queue again
            if (cyclic)
            {
                // estimate next msg length; based on current msg length
                int estimatedNextMsgEndPosition = currWritePos + msgLength * 2;
                if (estimatedNextMsgEndPosition > queueEndPos)
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

            lastMsgPosition = currMsgStart;
            lastFinishedWritePos = currWritePos;

            setLastMsgPosition(lastMsgPosition);
            
            long msgCount = getMsgCount() + 1;
            setMsgCount(msgCount);
        }
    }

    /**
     * Tailer that reads the message queue (aka iterator)
     * Many tailers can be used to read from this queue
     */
    public class OffHeapQueueTailer implements QueueTailer
    {
        private boolean readingMode = true;

        private int lastReadingPos = 0;

        private int currMsgStart = 0;

        private int currMsgLength = 0;

        private int nextMsgPosition = 0;

        private int currReadingPos = 0;

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

        private int readMsgLength()
        {
            int v = buffer.getInt(currReadingPos);
            currReadingPos += Integer.BYTES;

            return v;
        }

        private int readNextMsgPosition()
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

            // update currReadingPos to the next msg pointer
            currReadingPos = nextMsgPosition;

            lastReadingPos = currReadingPos;

            msgReadCount++;
            currMsgLength = 0;
        }
    }
}
