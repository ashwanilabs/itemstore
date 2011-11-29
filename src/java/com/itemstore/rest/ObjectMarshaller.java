package com.itemstore.rest;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

/**
 * Class ObjectMarshaller is a java object content handler
 * @author Ashwani Priyedarshi
 */

@Provider
@Produces("application/x-java-serialized-object")
@Consumes("application/x-java-serialized-object")
public class ObjectMarshaller implements MessageBodyReader, MessageBodyWriter {

    /**
     * Method isReadble checks if a Java type implements the
     * java.io.Serializable interface
     * @param type
     * @param genericType
     * @param annotations
     * @param mediaType
     * @return true is object is serializable otherwise false
     */
    @Override
    public boolean isReadable(Class type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return Serializable.class.isAssignableFrom(type);
    }

    /**
     * Method readFrom uses basic Java serialization to read a Java object
     * from the HTTP input stream
     * @param type
     * @param genericType
     * @param annotations
     * @param mediaType
     * @param httpHeaders
     * @param is
     * @return a Java object
     * @throws IOException
     * @throws WebApplicationException
     */
    @Override
    public Object readFrom(Class type, Type genericType, Annotation[] annotations, MediaType mediaType,
            MultivaluedMap httpHeaders, InputStream is) throws IOException, WebApplicationException {
        ObjectInputStream ois = new ObjectInputStream(is);
        try {
            return ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Method isWriteble checks if a Java type implements the
     * java.io.Serializable interface
     * @param type
     * @param genericType
     * @param annotations
     * @param mediaType
     * @return true is object is serializable otherwise false
     */
    @Override
    public boolean isWriteable(Class type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return Serializable.class.isAssignableFrom(type);
    }

    @Override
    public long getSize(Object o, Class type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    /**
     * Method writeto uses basic Java serialization is used to marshal the
     * Java object into the HTTP response body
     * @param o
     * @param type
     * @param genericType
     * @param annotations
     * @param mediaType
     * @param httpHeaders
     * @param os
     * @throws IOException
     * @throws WebApplicationException
     */
    @Override
    public void writeTo(Object o, Class type, Type genericType, Annotation[] annotations,
            MediaType mediaType, MultivaluedMap httpHeaders, OutputStream os) throws IOException, WebApplicationException {
        ObjectOutputStream oos = new ObjectOutputStream(os);
        oos.writeObject(o);
    }
}
