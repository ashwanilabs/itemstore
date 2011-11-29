package com.itemstore.rest;

import com.itemstore.config.AppConfig;
import com.itemstore.stores.ObjectStore;
import java.io.Serializable;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * REST Web Service
 * @author Ashwani Priyedarshi
 * Class ObjectResource implements the REST methods related to
 * items of type objects which are supported by the system
 */
public class ObjectResource implements ItemResource {

    UriInfo uriInfo;
    PathSegment itemInfo;
    String bucket;
    String user;
    String item;
    String type;
    int repCount;

    public ObjectResource(String item, String bucket, String user, String type, String rep, UriInfo uriInfo) {
        System.out.println("ObjectStore::Initializing ObjectResource::bucket=" + bucket + ":item=" + item);
        this.uriInfo = uriInfo;
        this.item = item;
        this.bucket = bucket;
        this.user = user;
        this.type = type;
        this.repCount = AppConfig.getReplicaCount();
        try {
            if (rep != null && !rep.isEmpty()) {
                this.repCount = Integer.parseInt(rep);
            }
        } catch (Exception e) {
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }
    }

    /**
     * Method getItem implements the GET method for the requested object
     * @return Response code 200 and the requested serialized object
     */
    @GET
    @Produces("application/x-java-serialized-object")
    public Response getItem() {
        System.out.println("ObjectStore::Received GET request::bucket=" + bucket + ":item=" + item);
        ObjectStore store = getStore(null);
        return Response.ok(store.get(item)).build();
    }

    /**
     * Method postItem implements the POST method for the new object
     * @param data : Serialized data
     * @return Response code 200 if object is successfully saved on the server
     */
    @POST
    @Consumes("application/x-java-serialized-object")
    public Response postItem(Serializable data) {
        System.out.println("ObjectStore::Received POST request::bucket=" + bucket + ":item=" + item);
        ObjectStore store = getStore(data);
        store.post(item);
        return Response.created(uriInfo.getAbsolutePath()).build();
    }

    /**
     * Method putItem implements the PUT method to update an existing object
     * @param data : Serialized data
     * @return Response code 200 if file is successfully updated on the server
     */
    @PUT
    @Consumes("application/x-java-serialized-object")
    public Response putItem(Serializable data) {
        System.out.println("ObjectStore::Received PUT request::bucket=" + bucket + ":item=" + item);
        ObjectStore store = getStore(data);
        store.put(item);
        return Response.ok(uriInfo.getAbsolutePath()).build();
    }

    /**
     * Method deleteItem implements the DELETE method for an existing object
     * @return Response code 200 if object is successfully deleted on the server
     */
    @DELETE
    public Response deleteItem() {
        System.out.println("ObjectStore::Received DELETE request::bucket=" + bucket + ":item=" + item);
        ObjectStore store = getStore(null);
        store.delete(item);
        return Response.ok().build();
    }

    private ObjectStore getStore(Serializable data) {
        if (type.equals("s") || type.equals("g") || type.equals("hg")) {
            return new ObjectStore(bucket, user, type, repCount, uriInfo, data);
        } else {
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }
    }
}
