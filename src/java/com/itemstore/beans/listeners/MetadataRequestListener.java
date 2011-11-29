package com.itemstore.beans.listeners;

import com.itemstore.beans.entities.Bucket;
import com.itemstore.beans.entities.Item;
import com.itemstore.config.AppConfig;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import javax.ejb.MessageDriven;
import javax.ejb.MessageDrivenContext;
import javax.ejb.ActivationConfigProperty;
import javax.jms.MessageListener;
import javax.jms.Message;
import javax.jms.JMSException;
import javax.annotation.Resource;
import java.util.logging.Logger;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;
import javax.persistence.TemporalType;

/**
 * @author Ashwani Priyedarshi
 * Message Bean Class MetadataRequestListener
 * MetadataRequestListener listens to the request for metadata updates.
 */
@MessageDriven(mappedName = "jms/Queue", activationConfig = {
    @ActivationConfigProperty(propertyName = "messageSelector", propertyValue = "type = 'mreq'"),
    @ActivationConfigProperty(propertyName = "acknowledgeMode", propertyValue = "Auto-acknowledge"),
    @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue")
})
public class MetadataRequestListener implements MessageListener {

    static final Logger logger = Logger.getLogger("MetadataRequestListener");
    @Resource
    private MessageDrivenContext mdc;
    private EntityManager em;
    private static final EntityManagerFactory emf = AppConfig.getEmf();

    public MetadataRequestListener() {
    }

    /**
     * Method onMessage processes the incoming message requests.
     * @param inMessage
     */
    @Override
    public void onMessage(Message inMessage) {

        em = emf.createEntityManager();

        try {
            if (inMessage instanceof TextMessage) {
                TextMessage txtmsg = (TextMessage) inMessage;
                String msgText = txtmsg.getText();
                String server = msgText.substring(0, msgText.indexOf(':'));
                System.out.println("ITEMSTORE::MetadataRequestListener::Received message from server=" + server);
                Long time = Long.parseLong(msgText.substring(msgText.indexOf(':') + 1));
                Query q = em.createQuery("SELECT i FROM ITEM i WHERE i.lastModified > :time").setParameter("time", new Date(time), TemporalType.DATE);
                List<Item> items = q.getResultList();
                String updates = "";
                updates += items.size();
                for (Item i : items) {
                    updates += "\n" + i.getString();
                }
                q = em.createQuery("SELECT b FROM BUCKET b WHERE b.lastModified > :time").setParameter("time", new Date(time), TemporalType.DATE);
                List<Bucket> buckets = q.getResultList();
                updates += "\n" + buckets.size();
                for (Bucket b : buckets) {
                    updates += "\n" + b.getString();
                }

                //Reply
                System.out.println("ITEMSTORE::MetadataRequestListener::Sending Reply to server=" + server);
                try {
                    Context c = new InitialContext();
                    QueueConnectionFactory qcf = (QueueConnectionFactory) c.lookup("jms/" + server + "ConnectionFactory");
                    Queue requestQ = (Queue) c.lookup("jms/Queue");
                    QueueConnection qconn = qcf.createQueueConnection();
                    QueueSession qsession = qconn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
                    QueueSender sender = qsession.createSender(requestQ);
                    txtmsg = qsession.createTextMessage(updates);
                    txtmsg.setStringProperty("type", "mres");
                    sender.send(txtmsg);
                    qconn.close();
                } catch (JMSException ex) {
                    Logger.getLogger(AppConfig.class.getName()).log(Level.SEVERE, null, ex);
                } catch (NamingException ex) {
                    Logger.getLogger(AppConfig.class.getName()).log(Level.SEVERE, null, ex);
                }
                System.out.println("ITEMSTORE::MetadataRequestListener::Reply=" + updates);
            } else {
                System.out.println("ITEMSTORE::MetadataRequestListener::Message of wrong type: " + inMessage.getClass().getName());
            }
        } catch (JMSException ex) {
            Logger.getLogger(MetadataRequestListener.class.getName()).log(Level.SEVERE, null, ex);
            mdc.setRollbackOnly();
        }

        em.close();
    }
}
