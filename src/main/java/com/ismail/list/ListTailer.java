package com.ismail.list;

/**
 * Tailer that reads the message queue (aka iterator)
 * Many tailers can be used to read from this queue
 * 
 * @author ismail
 * @since 20221017
 */
public interface ListTailer
{
    /**
     * Moves the pointer to the next message
     * This is used when reading the list 
     * 
     * @return true if there is a next message; otherwise false
     */
    public boolean nextMessage();

    /**
     * Moves the pointer to the first message
     * 
     * @return true if the list is not empty; otherwise false
     */
    public boolean seekFirst();
    
    /**
     * Moves the pointer to the last message
     * 
     * @return true if the list is not empty; otherwise false
     */
    public boolean seekLast();
    
    /**
     * Moves the pointer to the message at the given index
     * 
     * Note that the index is of type (long); because the OffHeap memory can be large
     * however; it depends on the implementation; i.e.
     * - OffHeapUnsafeList: uses Long
     * - OffHeapDirectByteBufferList: uses Integer
     * 
     * @return true if there is a message at the given index; otherwise false
     */
    public boolean seekToIndex(long index);
    
    

    public int readMsgLength();

    public byte readByte();

    public char readChar();

    public long readLong();

    public int readInt();

    public double readDouble();

    public boolean readBoolean();

    public String readString();

    public Object readObject();

    public void finish();
}