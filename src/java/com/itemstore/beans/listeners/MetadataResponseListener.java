package com.itemstore.beans.listeners;

import com.itemstore.beans.entities.Bucket;
import com.itemstore.beans.entities.Item;
import com.itemstore.config.AppConfig;
import com.itemstore.config.CheckPointer;
import java.util.Date;
import java.util.Scanner;
import java.util.logging.Level;
import javax.ejb.MessageDriven;
import javax.ejb.MessageDrivenContext;
import javax.ejb.ActivationConfigProperty;
import javax.jms.MessageListener;
import javax.jms.Message;
import javax.jms.JMSException;
import javax.annotation.Resource;
import java.util.logging.Logger;
import javax.jms.TextMessage;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;

/**
 * @author Ashwani Priyedarshi
 * Message Bean Class MetadataResponseListener
 * MetadataResponseListener listens to the metadata responses.
 * to buckets.
 */
@MessageDriven(mappedName = "jms/Queue", activationConfig = {
    @ActivationConfigProperty(propertyName = "messageSelector", propertyValue = "type = 'mres'"),
    @ActivationConfigProperty(propertyName = "acknowledgeMode", propertyValue = "Auto-acknowledge"),
    @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue")
})
public class MetadataResponseListener implements MessageListener {

    static final Logger logger = Logger.getLogger("MetadataResponseListener");
    @Resource
    private MessageDrivenContext mdc;
    private EntityManager em;
    private EntityTransaction et;
    private static final EntityManagerFactory emf = AppConfig.getEmf();
    private static String SERVER_LOCAL = AppConfig.getLocalServer();

    public MetadataResponseListener() {
    }

    /**
     * Method onMessage processes the incoming message requests.
     * @param inMessage
     */
    @Override
    public void onMessage(Message inMessage) {

        em = emf.createEntityManager();
        et = em.getTransaction();

        if (inMessage instanceof TextMessage) {

            String updates = "";
            try {
                TextMessage txtmsg = (TextMessage) inMessage;
                updates = txtmsg.getText();
            } catch (JMSException ex) {
                Logger.getLogger(MetadataResponseListener.class.getName()).log(Level.SEVERE, null, ex);
                mdc.setRollbackOnly();
            }

            System.out.println("ITEMSTORE::MetadataResponseListener::Updates: \n" + updates);
            Scanner s = new Scanner(updates);

            //Update items
            int itemUpdates = Integer.parseInt(s.nextLine());
            System.out.println("ITEMSTORE::MetadataResponseListener::Item Updates: " + itemUpdates);
            for (int i = 0; i < itemUpdates; i++) {
                Item newItem = new Item(s.nextLine());
                System.out.println("ITEMSTORE::MetadataResponseListener::Item " + (i + 1) + " : " + newItem);
                Item oldItem = em.find(Item.class, new Item.ItemId(newItem.getItemnm(), newItem.getBucketnm(), newItem.getUserid()));
                if (oldItem == null) {
                    et.begin();
                    em.persist(newItem);
                    et.commit();
                } else {
                    et.begin();
                    oldItem.merge(newItem);
                    if (oldItem.getServers().indexOf(SERVER_LOCAL + ":") != -1 || oldItem.getServers().indexOf(":" + SERVER_LOCAL) != -1) {
                        oldItem.setLastModified(new Date(1));
                    }
                    em.merge(oldItem);
                    et.commit();
                }
            }

            //Update buckets
            int bucketUpdates = Integer.parseInt(s.nextLine());
            System.out.println("ITEMSTORE::MetadataResponseListener::Bucket Updates: " + bucketUpdates);
            for (int i = 0; i < bucketUpdates; i++) {
                Bucket newBucket = new Bucket(s.nextLine());
                System.out.println("ITEMSTORE::MetadataResponseListener::Bucket " + (i + 1) + " : " + newBucket);
                Bucket oldBucket = em.find(Bucket.class, new Bucket.BucketId(newBucket.getBucketnm(), newBucket.getUserid()));
                if (oldBucket == null) {
                    et.begin();
                    em.persist(newBucket);
                    et.commit();
                } else {
                    et.begin();
                    oldBucket.merge(newBucket);
                    em.merge(oldBucket);
                    et.commit();
                }
            }

            em.close();

            //Start CheckPointer
            Thread t = new Thread(new CheckPointer());
            t.start();

        } else {
            System.out.println("ITEMSTORE::MetadataResponseListener::Message of wrong type: " + inMessage.getClass().getName());
        }

    }
}
