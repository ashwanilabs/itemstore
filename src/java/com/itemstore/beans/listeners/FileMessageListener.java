package com.itemstore.beans.listeners;

import com.itemstore.beans.entities.Item;
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
 * Message Bean Class FileMessageListener
 * FileMessageListener listens to the update message related
 * to files.
 */
@MessageDriven(mappedName = "jms/Queue", activationConfig = {
    @ActivationConfigProperty(propertyName = "messageSelector", propertyValue = "type = 'f'"),
    @ActivationConfigProperty(propertyName = "acknowledgeMode", propertyValue = "Auto-acknowledge"),
    @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue")
})
public class FileMessageListener implements MessageListener {

    static final Logger logger = Logger.getLogger("FileMessageListener");
    @Resource
    private MessageDrivenContext mdc;
    private static final EntityManagerFactory emf = AppConfig.getEmf();
    private EntityManager em;
    private EntityTransaction et;

    public FileMessageListener() {
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

            String method = inMessage.getStringProperty("method");

            if (method.equals("post") || method.equals("put")) {

                if (inMessage instanceof ObjectMessage) {

                    ObjectMessage objmsg = (ObjectMessage) inMessage;
                    if (objmsg.getObject() instanceof Item) {

                        Item item = (Item) objmsg.getObject();
                        System.out.println("ITEMSTORE::FileMessageListener::Received Post Request::" + item);

                        et.begin();
                        Item currItem = em.find(Item.class, new Item.ItemId(item.getItemnm(), item.getBucketnm(), item.getUserid()));
                        if (currItem == null && method.equals("post")) {
                            em.persist(item);
                        } else if (currItem != null){
                            currItem.merge(item);
                            em.merge(currItem);
                        }
                        em.flush();
                        et.commit();
                    }
                } else if (inMessage instanceof TextMessage) {

                    TextMessage txtmsg = (TextMessage) inMessage;

                    Item item = new Item(txtmsg.getText());

                    et.begin();
                    Item currItem = em.find(Item.class, new Item.ItemId(item.getItemnm(), item.getBucketnm(), item.getUserid()));
                    if (currItem == null && method.equals("post")) {
                        em.persist(item);
                    }else if (currItem != null) {
                        currItem.merge(item);
                        em.merge(currItem);
                    }
                    em.flush();
                    et.commit();

                }
            } else if (method.equals("delete")) {

                if (inMessage instanceof TextMessage) {

                    TextMessage txtmsg = (TextMessage) inMessage;
                    String bucketnm = txtmsg.getStringProperty("bucketnm");
                    String userid = txtmsg.getStringProperty("userid");
                    String itemnm = txtmsg.getText();

                    System.out.println("ITEMSTORE::FileMessageListener::Received Delete Request::item=" + itemnm + ":bucket=" + bucketnm);

                    et.begin();
                    Item localItem = em.find(Item.class, new Item.ItemId(itemnm, bucketnm, userid));
                    if (localItem == null) {
                        System.out.println("ITEMSTORE::FileMessageListener::item=" + itemnm + ":bucket=" + bucketnm + " NOT FOUND");
                    } else {
                        em.remove(localItem);
                        em.flush();
                    }
                    et.commit();

                }

            } else {
                System.out.println("ITEMSTORE::FileMessageListener::Message of wrong type: " + inMessage.getClass().getName());
            }
        } catch (JMSException ex) {
            Logger.getLogger(FileMessageListener.class.getName()).log(Level.SEVERE, null, ex);
            mdc.setRollbackOnly();
        }

        em.close();

    }
}
