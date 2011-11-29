package com.itemstore.rest;

import com.itemstore.config.AppConfig;
import com.itemstore.stores.FileStore;
import java.io.InputStream;
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
 * Class FileResource implements the REST methods related to
 * items of type files
 */
public class FileResource implements ItemResource {

    UriInfo uriInfo;
    PathSegment itemInfo;
    String bucket;
    String user;
    String item;
    String type;
    int repCount;

    public FileResource(String item, String bucket, String user, String type, String rep, UriInfo uriInfo) {
        System.out.println("ITEMSTORE::Initializing FileResource::bucket=" + bucket + ":item=" + item);
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
     * Method getItem implements the GET method for the requested file
     * @return Response code 200 and the requested file
     */
    @GET
    @Produces("application/octet-stream")
    public Response getItem() {
        System.out.println("ITEMSTORE::Received getItem request::bucket=" + bucket + ":item=" + item);
        FileStore store = new FileStore(bucket, user, type, repCount, uriInfo, null);
        return Response.ok(store.get(item)).build();
    }

    /**
     * Method postItem implements the POST method for the new file
     * @param in : InputStream pointing to the file stream
     * @return Response code 200 if file is successfully saved on the server
     */
    @POST
    @Consumes({"application/octet-stream"})
    public Response postItem(InputStream in) {
        System.out.println("ITEMSTORE::Received putItem request::bucket=" + bucket + ":item=" + item);
        FileStore store = new FileStore(bucket, user, type, repCount, uriInfo, in);
        store.post(item);
        return Response.created(uriInfo.getAbsolutePath()).build();
    }

    /**
     * Method putItem implements the PUT method to update an existing file
     * @param in : InputStream pointing to the file stream
     * @return Response code 200 if file is successfully updated on the server
     */
    @PUT
    @Consumes({"application/octet-stream"})
    public Response putItem(InputStream in) {
        System.out.println("ITEMSTORE::Received putItem request::bucket=" + bucket + ":item=" + item);
        FileStore store = new FileStore(bucket, user, type, repCount, uriInfo, in);
        store.put(item);
        return Response.created(uriInfo.getAbsolutePath()).build();
    }

    /**
     * Method deleteItem implements the DELETE method for an existing file
     * @return Response code 200 if file is successfully deleted on the server
     */
    @DELETE
    public Response deleteItem() {
        System.out.println("ITEMSTORE::Received deleteItem request::bucket=" + bucket + ":item=" + item);
        FileStore store = new FileStore(bucket, user, type, repCount, uriInfo, null);
        store.delete(item);
        return Response.ok().build();
    }
}
