package com.ismail.queue.offheap;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ismail.queue.ObjectSerializer;
import com.ismail.queue.ObjectSerializerSimple;

/**
 * OffHeap Memory Mapped File Queue
 * 
 * @author ismail
 * @since 20221020
 */
public class OffHeapMMFQueue<T>
{
    private final static Logger log = LoggerFactory.getLogger(OffHeapMMFQueue.class);

    protected static int WRITE_PAGE_SIZE = 10 * 1024 * 1024; 

    protected static int READ_PAGE_SIZE = 10 * 1024 * 1024; 

    private String fileName;
    
    //random access file
    private RandomAccessFile file;

    //channel returned from random access file
    private FileChannel channel;

    // buffer used to read
    private MappedByteBuffer readMbb;

    // buffer used to wirte
    private MappedByteBuffer writeMbb;

    // If the persistent file has backlog already
    private boolean backlogAvailable = false;

    //1 byte for the status of the message, 4 bytes length of the payload
    final private ByteBuffer header = ByteBuffer.allocateDirect(5);

    //1KB Message Packet
    final private ByteBuffer data = ByteBuffer.allocateDirect(1024);

    final private int packetCapacity = header.capacity() + data.capacity();

    final private int headerLength = 5;

    //absolute file read position
    private int fileReadPosition = 0;

    //absolute file write position
    private int fileWritePosition = 0;

    private ObjectSerializer objectSerializer;

    private Lock lock = new ReentrantLock();

    public OffHeapMMFQueue(String fileName)
    {
        this(fileName, null);
    }

    public OffHeapMMFQueue(String fileName, ObjectSerializer objectSerializer)
    {
        this.fileName = fileName;
        this.objectSerializer = objectSerializer;

        if (objectSerializer != null)
        {
            this.objectSerializer = objectSerializer;
        }
        else
        {
            this.objectSerializer = new ObjectSerializerSimple(1024);
        }
        
        init();
    }

    private void init()
    {
        try
        {
            file = new RandomAccessFile(fileName, "rw");
            channel = file.getChannel();
            initialiseReadBuffer();
            initialiseWriteBuffer();
        }
        catch (Throwable e)
        {
            log.error("FATAL : Couldn't initialise the persistent queue, overload protection won't work ", e);
            throw new RuntimeException("error initializing queue: "+ e.getMessage(), e);
        }
    }

    private void initialiseReadBuffer() throws IOException
    {
        //create the read buffer with fileReadPosition 0 initially
        readMbb = channel.map(FileChannel.MapMode.READ_WRITE, fileReadPosition, READ_PAGE_SIZE);
        int position = readMbb.position();
        byte active = readMbb.get(); //first byte to see whether the message is already read or not
        int length = readMbb.getInt();//next four bytes to see the data length

        while (active == 0 && length > 0)
        {
            // message is non active means, its read,so skipping it
            if (position + packetCapacity > readMbb.capacity())
            {
                // Bytebuffer is out of capacity, hence changing the buffer
                fileReadPosition = fileReadPosition + position + headerLength + length;
                readMbb = channel.map(FileChannel.MapMode.READ_WRITE, fileReadPosition, READ_PAGE_SIZE);
            }
            else
            {
                // skipping the read bytes
                readMbb.position(position + headerLength + length);
            }

            position = readMbb.position();
            active = readMbb.get();
            length = readMbb.getInt();
        }

        if (active == 1)
        {
            // the file has unconsumed message(s)
            backlogAvailable = true;
        }

        readMbb.position(position);
    }

    private void initialiseWriteBuffer() throws IOException
    {
        int position;
        byte active;
        int length;

        //start from the readposition, since we dont overwrite!
        writeMbb = channel.map(FileChannel.MapMode.READ_WRITE, fileReadPosition, WRITE_PAGE_SIZE);
        position = writeMbb.position();
        active = writeMbb.get();
        length = writeMbb.getInt();

        while (length > 0)
        {
            // message is there, so skip it, keep doing until u get the end
            if (position + packetCapacity > readMbb.capacity())
            {
                // over run capacity, hence move the paging
                fileWritePosition = fileWritePosition + position + headerLength + length;

                writeMbb = channel.map(FileChannel.MapMode.READ_WRITE, fileWritePosition, WRITE_PAGE_SIZE);
            }
            else
            {
                writeMbb.position(position + headerLength + length);
            }

            position = writeMbb.position();
            active = writeMbb.get();
            length = writeMbb.getInt();
        }

        writeMbb.position(position);
    }

    /**
     * Changing the read write positiong back to earlier
     */
    public void cleanUp()
    {
        lock.lock();
        try
        {
            channel.truncate(0);
            readMbb = channel.map(FileChannel.MapMode.READ_WRITE, 0, READ_PAGE_SIZE);
            writeMbb = channel.map(FileChannel.MapMode.READ_WRITE, 0, WRITE_PAGE_SIZE);

            backlogAvailable = false;

            fileReadPosition = readMbb.position();
            fileWritePosition = writeMbb.position();
        }
        catch (IOException e)
        {
            log.error("Error while trying to truncate the file ", e);

            fileReadPosition = readMbb.position();
            fileWritePosition = writeMbb.position();
        }
        finally
        {
            lock.unlock();
        }

    }

    /**
     * This queue has to be with consume, otherwise it gets corrupted
     */
    public boolean put(T t)
    {
        lock.lock();
        try
        {
            byte[] oBytes = objectSerializer.serialize(t);

            int length = oBytes.length;

            //prepare the header
            header.clear();
            header.put((byte) 1);
            header.putInt(length);
            header.flip();

            //prepare the data
            data.clear();
            data.put(oBytes);
            data.flip();

            int currentPosition = writeMbb.position();

            if (writeMbb.remaining() < packetCapacity)
            {
                //check weather current buffer is enuf, otherwise we need to change the buffer
                writeMbb.force();

                fileWritePosition = fileWritePosition + currentPosition;

                writeMbb = channel.map(READ_WRITE, fileWritePosition, WRITE_PAGE_SIZE);
            }

            writeMbb.put(header); //write header
            writeMbb.put(data); //write data

            return true;
        }
        catch (Throwable e)
        {
            log.error("error adding message: " + e.getMessage(), e);

            return false;
        }
        finally
        {
            lock.unlock();
        }
    }

    public T get()
    {
        lock.lock();
        try
        {
            // Loop through the messages, skip read messages, until we find 
            while (true)
            {
                int currentPosition = readMbb.position();

                if (readMbb.remaining() < packetCapacity)
                {
                    fileReadPosition = fileReadPosition + currentPosition;
                    readMbb = channel.map(READ_WRITE, fileReadPosition, READ_PAGE_SIZE);
                    currentPosition = readMbb.position();
                }

                // Read message header

                final byte active = readMbb.get();
                final int length = readMbb.getInt();

                // if message is read; jump to next one
                if (active == 0 && length > 0)
                {
                    readMbb.position(currentPosition + headerLength + length);

                    continue;
                }
                // if no message; then stop
                else if (length <= 0)
                {
                    //the queue is empty
                    readMbb.position(currentPosition);

                    return null;
                }
                // we got a message
                else
                {

                    byte[] bytes = new byte[length];
                    
                    readMbb.get(bytes);

                    //making it not active (deleted)
                    readMbb.put(currentPosition, (byte) 0);

                    T msg = (T) objectSerializer.deserialize(bytes);

                    if (log.isDebugEnabled())
                        log.debug("consume < " + msg);

                    return msg;
                }
            }
        }
        catch (Throwable e)
        {
            log.error("consume() Issue in reading the persistent queue : ", e);
            
            return null;
        }
        finally
        {
            lock.unlock();
        }
    }


    public boolean isEmpty()
    {
        lock.lock();
        try
        {
            int pos = readMbb.position();
            final byte b;
            final int length;
            try
            {
                b = readMbb.get(pos);
                length = readMbb.getInt(pos + 1);
            }
            catch (Throwable e)
            {
                return true;
            }
            if (b == 0 && length == 0)
            {
                backlogAvailable = false;
                return true;
            }
            else
            {
                return false;
            }

        }
        finally
        {
            lock.unlock();
        }
    }

    public long capacity()
    {
        return Long.MAX_VALUE; 
    }

    public boolean isBacklogAvailable()
    {
        return backlogAvailable;
    }

    public void close()
    {
        writeMbb.force();
        try
        {
            file.close();
        }
        catch (Throwable e)
        {
            //do nothing
        }
    }

}
