package com.ismail.util.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.commons.lang3.SerializationUtils;

public class SerializationUtil
{
    public static final byte[] serializeUsingJDK(Serializable v)
    {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); //
                ObjectOutputStream oos = new ObjectOutputStream(bos))
        {
            oos.writeObject(v);
            oos.flush();
            
            byte[] bb = bos.toByteArray();

            return bb;
        }
        catch (IOException io)
        {
            io.printStackTrace();

            throw new RuntimeException("Unable to serialize object: " + io.getMessage(), io);
        }
    }

    public static final Object deserializeUsingJDK(byte[] bb)
    {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bb); //
                ObjectInputStream in = new ObjectInputStream(bis))
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

    public static final byte[] serializeUsingApacheLang3(Serializable v)
    {
        byte[] data = SerializationUtils.serialize(v);
        return data;
    }

    public static final Object deserializeUsingApacheLang3(byte[] bb)
    {
        Object object = SerializationUtils.deserialize(bb);
        return object;
    }

    
    
    public static final byte[] serializeUsingIssy(Serializable v)
    {
        try (FastByteArrayOutputStream bos = new FastByteArrayOutputStream(); //
                ObjectOutputStream oos = new ObjectOutputStream(bos))
        {
            oos.writeObject(v);
            oos.flush();
            
            byte[] bb = bos.toByteArray();

            return bb;
        }
        catch (IOException io)
        {
            io.printStackTrace();

            throw new RuntimeException("Unable to serialize object: " + io.getMessage(), io);
        }
    }

    public static final Object deserializeUsingIssy(byte[] bb)
    {
        try (FastByteArrayInputStream bis = new FastByteArrayInputStream(bb); //
                ObjectInputStream in = new ObjectInputStream(bis))
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
