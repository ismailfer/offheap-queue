package com.ismail.queue.offheap;

import java.io.Serializable;

/**
 * Simple data object used for testing the list and queues
 * 
 * @author ismail
 * @since 20221019
 */
public class Data implements Serializable
{
    public char msgType = 'M';

    public int index = 0;

    public long time = 0L;

    public int nanos = 0;
    
    public double price = 0.0;

    public String notes = null;
}
