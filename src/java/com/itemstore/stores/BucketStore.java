package com.itemstore.stores;

import com.itemstore.beans.entities.Bucket;
import com.itemstore.beans.entities.Item;
import com.itemstore.config.AppConfig;
import com.itemstore.jaxb.Buckets;
import com.itemstore.jaxb.Items;
import com.itemstore.messenger.BucketMessenger;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Query;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * Class BucketStore implements the operations performed on the buckets
 * @author Ashwani Priyedarshi
 */
public class BucketStore {

    private static final String SERVER_LOCAL = AppConfig.getLocalServer();
    private static final EntityManagerFactory emf = AppConfig.getEmf();
    private EntityManager em;
    private EntityTransaction et;
    private URI uri;
    private String user;

    public BucketStore(String user, URI uri) {
        this.uri = uri;
        this.user = user;
    }

    /**
     * Method getBuckets queries the database to get the list of buckets and
     * returns a Buckets object
     * @return Object of type Buckets
     */
    public Buckets getBuckets() {
        em = emf.createEntityManager();
        Query q = em.createQuery("SELECT b FROM BUCKET b WHERE b.userid LIKE :user").setParameter("user", user);
        Buckets buckets = new Buckets(q.getResultList());
        em.close();
        return buckets;
    }

    /**
     * Method post create a new bucket in the local server and send the JMS
     * message with the new bucket information to other servers in the system
     * @param bucketnm
     */
    public void post(String bucketnm) {
        em = emf.createEntityManager();
        et = em.getTransaction();
        Bucket bucket = em.find(Bucket.class, new Bucket.BucketId(bucketnm, user));
        if (bucket == null) {

            et.begin();
            bucket = new Bucket(bucketnm, uri.toString(), 0, user, SERVER_LOCAL);
            em.persist(bucket);
            et.commit();

            em.close();

            //Send the messages to the other nodes
            Thread t = new Thread(new BucketMessenger(bucket));
            t.setName("post");
            t.start();

        } else {
            throw new WebApplicationException(Response.Status.CONFLICT);
        }
    }

    /**
     * Method put update an existing bucket in the local server and send the JMS
     * message with the updated bucket information to other servers in the system
     * @param bucketnm
     */
    public void put(String bucketnm) {
        em = emf.createEntityManager();
        et = em.getTransaction();
        Bucket bucket = em.find(Bucket.class, new Bucket.BucketId(bucketnm, user));
        if (bucket != null) {

            et.begin();
            bucket.setUserid(user);
            bucket.setLastModified(new GregorianCalendar().getTime());
            em.merge(bucket);
            et.commit();

            em.close();

            //Send the messages to the other nodes
            Thread t = new Thread(new BucketMessenger(bucket));
            t.setName("put");
            t.start();

        } else {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
    }

    /**
     * Method delete deletes the bucket in the local server and send the JMS
     * message to the delete the bucket to other servers in the system
     * @param bucketnm
     */
    public void delete(String bucketnm) {
        em = emf.createEntityManager();
        et = em.getTransaction();
        Bucket bucket = em.find(Bucket.class, new Bucket.BucketId(bucketnm, user));
        if (bucket != null) {

            et.begin();
            em.remove(bucket);
            et.commit();

            em.close();

            //Send the messages to the other nodes
            Thread t = new Thread(new BucketMessenger(bucket));
            t.setName("delete");
            t.start();


        } else {
            throw new WebApplicationException(Response.Status.CONFLICT);
        }
    }

    /**
     * Method get queries the database to find the items present in the bucket
     * and returns a object of type Items
     * @param bucketnm
     * @return Object of type Items
     */
    public Items get(String bucketnm) {
        em = emf.createEntityManager();
        Query q = em.createQuery("SELECT i FROM ITEM i WHERE i.bucketnm LIKE :bucket AND i.userid LIKE :user").setParameter("bucket", bucketnm).setParameter("user", user);
        Items Items = new Items(q.getResultList());
        em.close();
        return Items;
    }

    public ArrayList<Serializable> getObjs(String bucketnm) {
        em = emf.createEntityManager();
        Query q = em.createQuery("SELECT i FROM ITEM i WHERE i.bucketnm LIKE :bucket AND i.userid LIKE :user").setParameter("bucket", bucketnm).setParameter("user", user);
        List<Item> itemList = q.getResultList();
        em.close();
        ArrayList<Serializable> objList = new ArrayList<Serializable>();
        for (Item item : itemList) {
            objList.add(item.getBlob());
        }
        return objList;
    }
}
