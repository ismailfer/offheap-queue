package com.ismail.queue;

/**
 * Object serializer interface
 * 
 * Provides a customisable fast serializer / deserliazer
 * 
 * Users can choose to provide a faster implementation; i.e. re-used buffers, etc
 * 
 * @author ismail
 * @since 20221020
 */
public interface ObjectSerializer
{
    byte[] serialize(Object o);
    
    Object deserialize(byte[] bb);
    
    int serializeTo(Object o, byte[] buff, int offset);
    
    Object deserializeFrom(byte[] buff, int offset, int length);
}
