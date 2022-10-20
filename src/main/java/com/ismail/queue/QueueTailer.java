package com.ismail.queue;

/**
 * Tailer that reads the message queue (aka iterator)
 * Many tailers can be used to read from this queue
 * 
 * @author ismail
 * @since 20221017
 */
public interface QueueTailer
{
    public boolean nextMessage();

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