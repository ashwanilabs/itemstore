package com.itemstore.stores;

import com.itemstore.beans.entities.Item;
import com.itemstore.config.AppConfig;
import com.itemstore.messenger.ObjectMessenger;
import com.itemstore.types.ItemStoreObject;
import com.itemstore.types.Log;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.GregorianCalendar;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * Class ObjectStore implements the operations performed on the items of type
 * objects that are supported by the system
 * @author Ashwani Priyedarshi
 */
public class ObjectStore {

    private static String SERVER_LOCAL = AppConfig.getLocalServer();
    private static String[] SERVER_LIST = AppConfig.getServerList();
    private EntityManagerFactory emf = AppConfig.getEmf();
    private EntityManager em;
    private EntityTransaction et;
    private String bucket;
    private String user;
    private UriInfo uriInfo;
    private Serializable data;
    private String type;
    private int repCount;

    public ObjectStore(String bucket, String user, String type, int repCount, UriInfo uriInfo, Serializable data) {
        this.bucket = bucket;
        this.user = user;
        this.uriInfo = uriInfo;
        this.data = data;
        this.type = type;
        this.repCount = repCount;
    }

    /**
     * Method get reads the request object from the store if present locally or
     * redirects to the appropriate server
     * @param itemnm
     * @return the requested object
     */
    public Object get(String itemnm) {
        em = emf.createEntityManager();

        //Lookup the requested item
        Item item = em.find(Item.class, new Item.ItemId(itemnm, bucket, user));

        em.close();

        if (item == null) {//If item is not found

            throw new WebApplicationException(Response.Status.NOT_FOUND);

        } else if (item.getServers().indexOf(SERVER_LOCAL + ":") != -1 || item.getServers().indexOf(":" + SERVER_LOCAL) != -1) {//If item exist locally

            return item.getItem();

        } else { //If item doesn't exist locally, redirect to appropriate server

            String redirectServer = getNearestServer(item.getServers());
            System.out.println("ITEMSTORE::ObjectStore::Redirecting GET request::server=" + redirectServer + ":" + item);
            String uriString = uriInfo.getAbsolutePath().toString();
            uriString = uriString.replaceFirst(uriInfo.getAbsolutePath().getHost(), redirectServer);
            URI redirect = null;
            try {
                redirect = new URI(uriString);
            } catch (URISyntaxException ex) {
                Logger.getLogger(ObjectStore.class.getName()).log(Level.SEVERE, null, ex);
            }
            throw new WebApplicationException(Response.temporaryRedirect(redirect).status(Response.Status.TEMPORARY_REDIRECT).build());
        }
    }

    /**
     * Method post creates a new object in the system and sends the appropriate
     * JMS messages to other replicas and servers
     * @param itemnm
     */
    public void post(String itemnm) {

        //int hash = (user + ":" + bucket + ":" + itemnm).hashCode() % SERVER_NOS;
        //String[] server = AppConfig.getServerListString().split(":");

        em = emf.createEntityManager();
        et = em.getTransaction();
        //Lookup the requested item
        Item item = em.find(Item.class, new Item.ItemId(itemnm, bucket, user));

        if (item == null) {//Process the request if the item is new

            //if (SERVER_LOCAL.equals(server[hash])) {

                ItemStoreObject itemObj = (ItemStoreObject) data;

                String servers = SERVER_LOCAL + ":" + AppConfig.getRepServers(repCount - 1);

                //Update the metadata
                et.begin();
                item = new Item(itemnm, bucket, user, type, uriInfo.getAbsolutePath().toString(), SERVER_LOCAL, servers);
                item.setItem(itemObj);
                em.persist(item);
                em.flush();
                et.commit();

                em.close();

                //Send the messages to the other nodes
                Thread t = new Thread(new ObjectMessenger(item, null));
                t.setName("post");
                t.start();
            //} else {
            //    String redirectServer = server[hash];
            //    System.out.println("ITEMSTORE::ObjectStore::Redirecting GET request::server=" + redirectServer + ":" + item);
            //    String uriString = uriInfo.getAbsolutePath().toString();
            //    uriString = uriString.replaceFirst(uriInfo.getAbsolutePath().getHost(), redirectServer);
            //    URI redirect = null;
            //    try {
            //        redirect = new URI(uriString);
            //    } catch (URISyntaxException ex) {
            //        Logger.getLogger(ObjectStore.class.getName()).log(Level.SEVERE, null, ex);
            //    }
            //    throw new WebApplicationException(Response.temporaryRedirect(redirect).status(Response.Status.TEMPORARY_REDIRECT).build());
            //}

        } else {
            throw new WebApplicationException(Response.Status.CONFLICT);
        }
    }

    /**
     * Method put updates a object in the system and sends the appropriate
     * JMS messages to other replicas and servers
     * @param itemnm
     */
    public void put(String itemnm) {
        em = emf.createEntityManager();
        et = em.getTransaction();
        //Lookup the item
        Item item = em.find(Item.class, new Item.ItemId(itemnm, bucket, user));

        if (item == null) {//If item is not found
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        } else if (item.getServers().indexOf(SERVER_LOCAL + ":") != -1 || item.getServers().indexOf(":" + SERVER_LOCAL) != -1) {//If item exist locally

            Log log = (Log) data;

            //Update the metadata
            et.begin();
            item.setLastModified(new GregorianCalendar().getTime());
            ItemStoreObject itemObj = item.getItem();
            itemObj.applyLog(log);
            item.setItem(itemObj);
            em.merge(item);
            em.flush();
            et.commit();

            em.close();

            //Send the messages to the other nodes
            Thread t = new Thread(new ObjectMessenger(item, log));
            t.setName("put");
            t.start();


        } else { //If item doesn't exist locally, redirect to appropriate server

            String redirectServer = getNearestServer(item.getServers());
            System.out.println("ITEMSTORE::ObjectStore::Redirecting PUT request::server=" + redirectServer + ":" + item);
            String uriString = uriInfo.getAbsolutePath().toString();
            uriString = uriString.replaceFirst(uriInfo.getAbsolutePath().getHost(), redirectServer);
            URI redirect = null;
            try {
                redirect = new URI(uriString);
            } catch (URISyntaxException ex) {
                Logger.getLogger(ObjectStore.class.getName()).log(Level.SEVERE, null, ex);
            }
            throw new WebApplicationException(Response.temporaryRedirect(redirect).status(Response.Status.TEMPORARY_REDIRECT).build());
        }
    }

    /**
     * Method delete deletes a object in the system and sends the appropriate
     * JMS messages to other servers
     * @param itemnm
     */
    public void delete(String itemnm) {
        em = emf.createEntityManager();
        et = em.getTransaction();
        et.begin();
        Item item = em.find(Item.class, new Item.ItemId(itemnm, bucket, user));
        if (item != null) {
            em.remove(item);
            em.flush();
        }
        et.commit();

        em.close();

        //Update the other servers
        Thread t = new Thread(new ObjectMessenger(item, null));
        t.setName("delete");
        t.start();
    }

    /**
     * Method getNearestServer calulate the nearest replica server
     * with respect to the current server
     * @param servers
     * @return the nearest server id
     */
    private String getNearestServer(String servers) {

        String[] serverList = SERVER_LIST;
        for (int i = 0; i
                < serverList.length; i++) {
            if (servers.indexOf(serverList[i]) != -1) {
                return serverList[i];
            }
        }
        return serverList[0];
    }
}
