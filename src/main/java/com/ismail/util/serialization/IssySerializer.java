package com.ismail.util.serialization;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Cached serializer 
 * 
 * NOT thread safe
 * 
 * You should use Threadlocal to guarantee concurrency
 * 
 * @author ismail
 * @since 20221018
 */
public class IssySerializer
{
    private FastByteArrayOutputStream bos = null;

    private FastByteArrayInputStream bis = null;

    public IssySerializer(int capacity)
    {
        bos = new FastByteArrayOutputStream(capacity);
        bis = new FastByteArrayInputStream(capacity);
    }

    public final byte[] serialize(Serializable v)
    {
        try (ObjectOutputStream oos = new ObjectOutputStream(bos))
        {
            oos.writeObject(v);
            oos.flush();

            byte[] bb = bos.toByteArray();

            bos.reset();

            return bb;
        }
        catch (IOException io)
        {
            io.printStackTrace();

            throw new RuntimeException("Unable to serialize object: " + io.getMessage(), io);
        }
    }

    public final int serializeIntoBuff(Serializable v, byte[] buff, int offset)
    {
        try (ObjectOutputStream oos = new ObjectOutputStream(bos))
        {
            oos.writeObject(v);
            oos.flush();

            int size = bos.toByteArrayCopy(buff, offset);

            bos.reset();

            return size;
        }
        catch (IOException io)
        {
            io.printStackTrace();

            throw new RuntimeException("Unable to serialize object: " + io.getMessage(), io);
        }
    }

    public final Object deserialize(byte[] bb)
    {
        bis.init(bb);
        
        try (ObjectInputStream in = new ObjectInputStream(bis))
        {
            Object v = in.readObject();

            return v;
        }
        catch (IOException io)
        {
            throw new RuntimeException("Unable to deserialize object: " + io.getMessage(), io);
        }
        catch (ClassNotFoundException io)
        {
            throw new RuntimeException("Unable to deserialize object: " + io.getMessage(), io);
        }
    }

    public final Object deserializeFromByteRange(byte[] bb, int offset, int length)
    {
        bis.init(bb, offset, length);

        try (ObjectInputStream in = new ObjectInputStream(bis))
        {
            Object v = in.readObject();

            return v;
        }
        catch (IOException io)
        {
            throw new RuntimeException("Unable to deserialize object: " + io.getMessage(), io);
        }
        catch (ClassNotFoundException io)
        {
            throw new RuntimeException("Unable to deserialize object: " + io.getMessage(), io);
        }
    }

}
