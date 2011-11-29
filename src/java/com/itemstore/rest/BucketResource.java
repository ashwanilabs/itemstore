package com.itemstore.rest;

import com.itemstore.stores.BucketStore;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * REST Web Service
 * @author Ashwani Priyedarshi
 * Class BucketResource implements the REST methods related to buckets
 */
public class BucketResource implements ItemResource {

    UriInfo uriInfo;
    String bucket;
    String user;
    String type;
    String rep;

    public BucketResource(String bucket, String user, String type, String rep, UriInfo uriInfo) {
        System.out.println("ITEMSTORE::Initializing BucketResource::bucket=" + bucket);
        this.uriInfo = uriInfo;
        this.bucket = bucket;
        this.user = user;
        this.type = type;
        this.rep = rep;
    }

    /**
     * Method getItems implements the GET method and returns the XML 
     * representation of the items present in a particular bucket
     * @return Response code 200 and data of mime-type "application/xml"
     */
    @GET
    @Produces("application/xml")
    public Response getItems() {
        System.out.println("ITEMSTORE::Received getItems request::bucket=" + bucket);
        return Response.ok(new BucketStore(user, uriInfo.getAbsolutePath()).get(bucket)).build();
    }

    /**
     * Method postBucket implements the POST method to create a new bucket
     * @return Response code 201 if bucket is successfully created
     */
    @POST
    public Response postBucket() {
        System.out.println("ITEMSTORE::Received putBucket request::bucket=" + bucket);
        new BucketStore(user, uriInfo.getAbsolutePath()).post(bucket);
        return Response.created(uriInfo.getAbsolutePath()).build();
    }

    /**
     * Method putBucket implements the PUT method to update a bucket
     * @return Response code 200 if bucket is successfully updated
     */
    @PUT
    public Response putBucket() {
        System.out.println("ITEMSTORE::Received putBucket request::bucket=" + bucket);
        new BucketStore(user, uriInfo.getAbsolutePath()).put(bucket);
        return Response.ok(uriInfo.getAbsolutePath()).build();
    }

    /**
     * Method deleteBucket implements the DELETE method to delete a bucket
     * @return Response code 200 if bucket is successfully deleted
     */
    @DELETE
    public Response deleteBucket() {
        System.out.println("ITEMSTORE::Received putBucket request::bucket=" + bucket);
        new BucketStore(user, uriInfo.getAbsolutePath()).delete(bucket);
        return Response.ok().build();
    }

    @Path("{item: .+}")
    public ItemResource getItemResource(@PathParam("item") String item) {
        if (!type.equals("f")) {
            return new ObjectResource(item, bucket, user, type, rep, uriInfo);
        } else {
            return new FileResource(item, bucket, user, type, rep, uriInfo);
        }
    }
}
