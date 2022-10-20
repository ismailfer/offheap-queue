package com.ismail.queue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * @author ismail
 * @since 20221020
 */
public class ObjectSerializerSimple implements ObjectSerializer
{
    //private ByteArrayOutputStream bos = null;

    private int bufferSize = 255;

    public ObjectSerializerSimple()
    {

    }

    public ObjectSerializerSimple(int bufferSize)
    {
        //bos = new ByteArrayOutputStream(bufferSize);
        this.bufferSize = bufferSize;
    }

    @Override
    public byte[] serialize(Object o)
    {
        //bos.reset();

        ByteArrayOutputStream bos = null;
        ObjectOutputStream oos = null;
        try
        {
            bos = new ByteArrayOutputStream(bufferSize);
            oos = new ObjectOutputStream(bos);
            oos.writeObject(o);
            oos.flush();
            byte[] bytes = bos.toByteArray();
            return bytes;
        }
        catch (IOException io)
        {
            throw new RuntimeException("Error serializing object: " + io.getMessage(), io);
        }
        finally
        {
            try
            {
                if (oos != null)
                {
                    oos.close();
                }
            }
            catch (Throwable ignore)
            {
                /* Do Nothing */
            }

            try
            {
                if (bos != null)
                {
                    bos.close();
                }
            }
            catch (Throwable ignore)
            {
                /* Do Nothing */
            }
        }
    }

    @Override
    public Object deserialize(byte[] bytes)
    {
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        try
        {
            bis = new ByteArrayInputStream(bytes);
            ois = new ObjectInputStream(bis);

            Object obj = ois.readObject();

            return obj;
        }
        catch (IOException io)
        {
            throw new RuntimeException("Error deserializing object: " + io.getMessage(), io);
        }
        catch (ClassNotFoundException io)
        {
            throw new RuntimeException("Error deserializing object: " + io.getMessage(), io);
        }
        finally
        {
            try
            {
                if (ois != null)
                {
                    ois.close();
                }
            }
            catch (Throwable ignore)
            {
                /* Do Nothing */
            }

            try
            {
                if (bis != null)
                {
                    bis.close();
                }
            }
            catch (Throwable ignore)
            {
                /* Do Nothing */
            }
        }
    }

    @Override
    public int serializeTo(Object o, byte[] buff, int offset)
    {
        //bos.reset();

        ByteArrayOutputStream bos = null;
        ObjectOutputStream oos = null;
        try
        {
            bos = new ByteArrayOutputStream(bufferSize);
            oos = new ObjectOutputStream(bos);
            oos.writeObject(o);
            oos.flush();
            
            int size = bos.size();

            byte[] bytes = bos.toByteArray();
            
            for (int i=0; i<size; i++)
                buff[offset+i] = bytes[i];
            
            return size;
            
        }
        catch (IOException io)
        {
            throw new RuntimeException("Error serializing object: " + io.getMessage(), io);
        }
        finally
        {
            try
            {
                if (oos != null)
                {
                    oos.close();
                }
            }
            catch (Throwable ignore)
            {
                /* Do Nothing */
            }

            try
            {
                if (bos != null)
                {
                    bos.close();
                }
            }
            catch (Throwable ignore)
            {
                /* Do Nothing */
            }
        }
    }

    @Override
    public Object deserializeFrom(byte[] buff, int offset, int length)
    {
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        try
        {
            bis = new ByteArrayInputStream(buff, offset, length);
            ois = new ObjectInputStream(bis);

            Object obj = ois.readObject();

            return obj;
        }
        catch (IOException io)
        {
            throw new RuntimeException("Error deserializing object: " + io.getMessage(), io);
        }
        catch (ClassNotFoundException io)
        {
            throw new RuntimeException("Error deserializing object: " + io.getMessage(), io);
        }
        finally
        {
            try
            {
                if (ois != null)
                {
                    ois.close();
                }
            }
            catch (Throwable ignore)
            {
                /* Do Nothing */
            }

            try
            {
                if (bis != null)
                {
                    bis.close();
                }
            }
            catch (Throwable ignore)
            {
                /* Do Nothing */
            }
        }
    }
}
