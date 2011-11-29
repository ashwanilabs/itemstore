package com.itemstore.rest;

import java.util.HashSet;
import java.util.Set;
import javax.ws.rs.core.Application;

/**
 * Class ItemStoreApplication
 * @author Ashwani Priyedarshi
 */
@javax.ws.rs.ApplicationPath("resources")
public class ItemStoreApplication extends Application {

    private Set<Object> singletons = new HashSet<Object>();
    private Set<Class<?>> classes = new HashSet<Class<?>>();

    public ItemStoreApplication() {
        singletons.add(new BucketsResource());
        classes.add(ObjectMarshaller.class);
    }

    /**
     * Method getClasses
     * @return a list of JAX-RS service classes
     */
    @Override
    public Set<Class<?>> getClasses() {
        return classes;
    }

    /**
     * Method getSingletons
     * @return a list of JAX-RS service objects
     */
    @Override
    public Set<Object> getSingletons() {
        return singletons;
    }
}
