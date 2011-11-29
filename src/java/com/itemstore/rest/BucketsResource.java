package com.itemstore.rest;

import com.itemstore.stores.BucketStore;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.Path;
import javax.ws.rs.GET;
import javax.ws.rs.MatrixParam;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;

/**
 * REST Web Service
 * @author Ashwani Priyedarshi
 * Class BucketsResource implements the REST methods to get the list of buckets
 */
@Path("/buckets")
public class BucketsResource {

    @Context
    UriInfo uriInfo;
    @Context
    Request request;

    public BucketsResource() {
        System.out.println("ITEMSTORE::Initializing BucketsResource");
    }

    /**
     * Method getBuckets implements the GET method and returns the XML
     * representation of the buckets
     * @return Response code 200 and data of mime-type "application/xml"
     */
    @GET
    @Produces("application/xml")
    public Response getBuckets(@MatrixParam("user") String user) {
        System.out.println("ITEMSTORE::Received getBuckets request");
        if (user != null && !user.isEmpty()) {
            return Response.ok(new BucketStore(user, uriInfo.getAbsolutePath()).getBuckets()).build();
        } else {
            return Response.ok("ON").build();
        }
    }

    @Path("{bucket}")
    public BucketResource getBucketResource(@PathParam("bucket") String bucket, @MatrixParam("user") String user, @MatrixParam("type") String type, @MatrixParam("rep") String rep) {
        return new BucketResource(bucket, user, type, rep, uriInfo);
    }
}
