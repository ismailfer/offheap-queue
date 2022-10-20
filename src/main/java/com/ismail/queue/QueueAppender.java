package com.ismail.queue;

import java.io.Serializable;

/**
 * Appender that writes messages to the queue
 * 
 * Only one appender should be used for this queue
 * 
 * First 8 bytes: message length in bytes
 * Next N bytes: message content
 * 
 * @author ismail
 * @since 20221017
 */
public interface QueueAppender
{
    void start();

    void writeMsgLength(int v);

    void writeMsgConsumed(boolean v);
    
    void writeNextMsgPosition(long v);
    
    void writeByte(byte v);

    void writeChar(char v);

    void writeLong(long v);

    void writeInt(int v);

    void writeDouble(double v);

    void writeBoolean(boolean v);

    void writeString(String v);

    void writeObject(Serializable v);


    void finish();
}