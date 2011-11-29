package com.itemstore.beans.listeners;

import com.itemstore.beans.entities.Bucket;
import com.itemstore.config.AppConfig;
import java.util.logging.Level;
import javax.ejb.MessageDriven;
import javax.ejb.MessageDrivenContext;
import javax.ejb.ActivationConfigProperty;
import javax.jms.MessageListener;
import javax.jms.Message;
import javax.jms.JMSException;
import javax.annotation.Resource;
import java.util.logging.Logger;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;

/**
 * @author Ashwani Priyedarshi
 * Message Bean Class BucketMessageListener
 * BucketMessageListener listens to the update message related
 * to buckets.
 */
@MessageDriven(mappedName = "jms/Queue", activationConfig = {
    @ActivationConfigProperty(propertyName = "messageSelector", propertyValue = "type = 'b'"),
    @ActivationConfigProperty(propertyName = "acknowledgeMode", propertyValue = "Auto-acknowledge"),
    @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue")
})
public class BucketMessageListener implements MessageListener {

    static final Logger logger = Logger.getLogger("BucketMessageListener");
    @Resource
    private MessageDrivenContext mdc;
    private EntityManager em;
    private EntityTransaction et;
    private static final EntityManagerFactory emf = AppConfig.getEmf();

    public BucketMessageListener() {
    }

    /**
     * Method onMessage processes the incoming message requests.
     * @param inMessage
     */
    @Override
    public void onMessage(Message inMessage) {

        em = emf.createEntityManager();
        et = em.getTransaction();

        try {
            if (inMessage instanceof ObjectMessage) {
                ObjectMessage objmsg = (ObjectMessage) inMessage;
                Bucket bucket = (Bucket) objmsg.getObject();
                Bucket currBucket = em.find(Bucket.class, new Bucket.BucketId(bucket.getBucketnm(), bucket.getUserid()));
                if (currBucket == null) {
                    et.begin();
                    em.persist(bucket);
                    et.commit();
                } else {
                    et.begin();
                    currBucket.merge(bucket);
                    em.merge(currBucket);
                    et.commit();
                }

            } else if (inMessage instanceof TextMessage) {
                TextMessage txtmsg = (TextMessage) inMessage;
                String bucketnm = txtmsg.getText();
                String user = txtmsg.getStringProperty("user");
                Bucket currBucket = em.find(Bucket.class, new Bucket.BucketId(bucketnm, user));
                if (currBucket != null) {
                    et.begin();
                    em.remove(currBucket);
                    et.commit();
                }

            } else {
                System.out.println("ITEMSTORE::BucketMessageListener::Message of wrong type: " + inMessage.getClass().getName());
            }
        } catch (JMSException ex) {
            Logger.getLogger(BucketMessageListener.class.getName()).log(Level.SEVERE, null, ex);
            mdc.setRollbackOnly();
        }

        em.close();
    }
}
