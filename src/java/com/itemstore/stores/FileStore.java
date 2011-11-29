package com.itemstore.stores;

import com.itemstore.beans.entities.Item;
import com.itemstore.config.AppConfig;
import com.itemstore.messenger.FileMessenger;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.io.IOUtils;

/**
 * Class FileStore implements the operations performed on the items of type
 * files
 * @author Ashwani Priyedarshi
 */
public class FileStore {

    private static String SERVER_LOCAL = AppConfig.getLocalServer();
    private static String[] SERVER_LIST = AppConfig.getServerList();
    private EntityManagerFactory emf = AppConfig.getEmf();
    private EntityManager em;
    private EntityTransaction et;
    private String bucket;
    private String user;
    private UriInfo uriInfo;
    private InputStream in;
    private String type;
    private int repCount;

    public FileStore(String bucket, String user, String type, int repCount, UriInfo uriInfo, InputStream in) {
        this.bucket = bucket;
        this.user = user;
        this.uriInfo = uriInfo;
        this.in = in;
        this.type = type;
        this.repCount = repCount;
    }

    /**
     * Method get reads the request file from the store if present locally or
     * redirects to the appropriate server
     * @param itemnm
     * @return A StreamingOutput object referring to the file
     */
    public StreamingOutput get(String itemnm) {
        em = emf.createEntityManager();
        Item item = em.find(Item.class, new Item.ItemId(itemnm, bucket, user));
        em.close();
        String servers = item.getServers();
        if (servers.isEmpty()) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        } else if (servers.indexOf(SERVER_LOCAL) != -1) {
            final byte[] file = item.getBlob();
            if (file != null) {
                return new StreamingOutput() {

                    @Override
                    public void write(OutputStream output) throws IOException, WebApplicationException {
                        output.write(file, 0, file.length);
                    }
                };
            } else {
                throw new WebApplicationException(Response.Status.NOT_FOUND);
            }
        } else {
            String redirectServer = getNearestServer(servers);
            System.out.println("ITEMSTORE::FileStore::Redirecting getItem request::server=" + redirectServer + ":" + item);
            String uriString = uriInfo.getAbsolutePath().toString();
            uriString = uriString.replaceFirst(uriInfo.getAbsolutePath().getHost(), redirectServer);
            URI redirect = null;
            try {
                redirect = new URI(uriString);
            } catch (URISyntaxException ex) {
                Logger.getLogger(FileStore.class.getName()).log(Level.SEVERE, null, ex);
            }
            throw new WebApplicationException(Response.temporaryRedirect(redirect).status(Response.Status.TEMPORARY_REDIRECT).build());
        }
    }

    /**
     * Method post creates a new file in the system and sends the appropriate
     * JMS messages to other replicas and servers
     * @param itemnm
     */
    public void post(String itemnm) {
        em = emf.createEntityManager();
        et = em.getTransaction();
        Item item = em.find(Item.class, new Item.ItemId(itemnm, bucket, user));
        if (item == null) {
            try {

                byte[] file = IOUtils.toByteArray(in);

                String servers = SERVER_LOCAL + ":" + AppConfig.getRepServers(repCount - 1);
                //Update the metadata
                et.begin();
                item = new Item(itemnm, bucket, user, type, uriInfo.getAbsolutePath().toString(), SERVER_LOCAL, servers);
                item.setBlob(file);
                em.persist(item);
                et.commit();

                em.close();

                //Send the messages to the other nodes
                Thread t = new Thread(new FileMessenger(item));
                t.setName("post");
                t.start();
            } catch (IOException ex) {
                Logger.getLogger(FileStore.class.getName()).log(Level.SEVERE, null, ex);
            }

        } else {
            throw new WebApplicationException(Response.Status.CONFLICT);
        }
    }

    /**
     * Method put updates a file in the system and sends the appropriate
     * JMS messages to other replicas and servers
     * @param itemnm
     */
    public void put(String itemnm) {
        em = emf.createEntityManager();
        et = em.getTransaction();
        Item item = em.find(Item.class, new Item.ItemId(itemnm, bucket, user));

        if (item != null) {
            try {

                byte[] file = IOUtils.toByteArray(in);
                //Update the metadata
                et.begin();
                item.setLastModified(new GregorianCalendar().getTime());
                item.setBlob(file);
                em.merge(item);
                et.commit();

                em.close();

                //Send the messages to the other nodes
                Thread t = new Thread(new FileMessenger(item));
                t.setName("put");
                t.start();
            } catch (IOException ex) {
                Logger.getLogger(FileStore.class.getName()).log(Level.SEVERE, null, ex);
            }

        } else { //If item doesn't exist locally, redirect to appropriate server
            String redirectServer = getNearestServer(item.getServers());
            System.out.println("ITEMSTORE::FileStore::Redirecting PUT request::server=" + redirectServer + ":" + item);
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
     * Method delete deletes a file in the system and sends the appropriate
     * JMS messages to other servers
     * @param itemnm
     */
    public void delete(String itemnm) {
        em = emf.createEntityManager();
        et = em.getTransaction();
        Item item = em.find(Item.class, new Item.ItemId(itemnm, bucket, user));

        if (item != null) {

            et.begin();
            em.remove(item);
            et.commit();

            em.close();

            //Send the messages to the other nodes
            Thread t = new Thread(new FileMessenger(item));
            t.setName("delete");
            t.start();

        } else {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
    }

    /**
     * Method getNearestServer calulate the nearest replica server
     * with respect to the current server
     * @param servers
     * @return the nearest server id
     */
    private String getNearestServer(String servers) {
        String[] serverList = SERVER_LIST;
        for (int i = 0; i < serverList.length; i++) {
            if (servers.indexOf(serverList[i]) != -1) {
                return serverList[i];
            }
        }
        return serverList[0];
    }
}
