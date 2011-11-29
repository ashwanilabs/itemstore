package com.itemstore.beans.listeners;

import com.itemstore.beans.entities.Item;
import com.itemstore.config.AppConfig;
import com.itemstore.types.Log;
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
import javax.persistence.EntityTransaction;

/**
 * @author Ashwani Priyedarshi
 * Message Bean Class ObjectMessageListener
 * ObjectMessageListener listens to the update message related
 * to objects supported by the system.
 */
@MessageDriven(mappedName = "jms/Queue", activationConfig = {
    @ActivationConfigProperty(propertyName = "messageSelector", propertyValue = "type = 'hg' OR type = 'g' OR type = 's'"),
    @ActivationConfigProperty(propertyName = "acknowledgeMode", propertyValue = "Auto-acknowledge"),
    @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue")
})
public class ObjectMessageListener implements MessageListener {

    static final Logger logger = Logger.getLogger("ObjectMessageListener");
    @Resource
    private MessageDrivenContext mdc;
    private static final String SERVER_LOCAL = AppConfig.getLocalServer();
    private static final EntityManagerFactory emf = AppConfig.getEmf();
    private EntityManager em;
    private EntityTransaction et;

    public ObjectMessageListener() {
    }

    /**
     * Method onMessage processes the incoming message requests.
     * @param inMessage
     */
    @Override
    public void onMessage(Message inMessage) {

        //Init entity manager
        em = emf.createEntityManager();
        et = em.getTransaction();

        try {
            String method = inMessage.getStringProperty("method");

            if (method.equals("post")) {

                if (inMessage instanceof ObjectMessage) {

                    ObjectMessage objmsg = (ObjectMessage) inMessage;

                    if (objmsg.getObject() instanceof Item) {

                        Item item = (Item) objmsg.getObject();
                        System.out.println("ITEMSTORE::ObjectMessageListener::Received Post Request::" + item);

                        et.begin();
                        Item currItem = em.find(Item.class, new Item.ItemId(item.getItemnm(), item.getBucketnm(), item.getUserid()));
                        if (currItem == null) {
                            em.persist(item);
                        } else {
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
                    if (currItem == null) {
                        em.persist(item);
                    } else {
                        currItem.merge(item);
                        em.merge(currItem);
                    }
                    em.flush();
                    et.commit();
                }

            } else if (method.equals("put")) {

                if (inMessage instanceof ObjectMessage) {

                    ObjectMessage objmsg = (ObjectMessage) inMessage;

                    if (objmsg.getObject() instanceof Log) {

                        Log log = (Log) objmsg.getObject();

                        String itemStr = objmsg.getStringProperty("item");
                        Item item = new Item(itemStr);

                        System.out.println("ITEMSTORE::ObjectMessageListener::Received Put Request::" + item);

                        et.begin();
                        Item localItem = em.find(Item.class, new Item.ItemId(item.getItemnm(), item.getBucketnm(), item.getUserid()));
                        if (localItem == null) {
                            System.out.println("ITEMSTORE::ObjectMessageListener::" + item + " NOT FOUND");
                        } else {

                            localItem.merge2(item, log);
                            em.merge(localItem);
                            em.flush();

                        }
                        et.commit();
                    }

                } else if (inMessage instanceof TextMessage) {

                    TextMessage txtmsg = (TextMessage) inMessage;

                    Item item = new Item(txtmsg.getText());

                    et.begin();
                    Item currItem = em.find(Item.class, new Item.ItemId(item.getItemnm(), item.getBucketnm(), item.getUserid()));
                    if (currItem == null) {
                        //em.persist(item);
                        System.out.println("ITEMSTORE::ObjectMessageListener::" + item + " NOT FOUND");
                    } else {
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

                    System.out.println("ITEMSTORE::ObjectMessageListener::Received Delete Request::item=" + itemnm + ":bucket=" + bucketnm);

                    et.begin();
                    Item localItem = em.find(Item.class, new Item.ItemId(itemnm, bucketnm, userid));
                    if (localItem == null) {
                        System.out.println("ITEMSTORE::ObjectMessageListener::item=" + itemnm + ":bucket=" + bucketnm + " NOT FOUND");
                    } else {
                        em.remove(localItem);
                        em.flush();
                    }
                    et.commit();

                }

            } else if (method.equals("merge")) {

                if (inMessage instanceof ObjectMessage) {

                    ObjectMessage objmsg = (ObjectMessage) inMessage;

                    if (objmsg.getObject() instanceof Item) {

                        Item item = (Item) objmsg.getObject();
                        System.out.println("ITEMSTORE::ObjectMessageListener::Received Merge Request::" + item);

                        et.begin();
                        Item currItem = em.find(Item.class, new Item.ItemId(item.getItemnm(), item.getBucketnm(), item.getUserid()));
                        if (currItem == null) {
                            em.persist(item);
                        } else {
                            currItem.merge(item);
                            em.merge(currItem);
                        }
                        em.flush();
                        et.commit();
                    }
                }
            } else if (method.equals("get")) {

                if (inMessage instanceof TextMessage) {

                    TextMessage txtmsg = (TextMessage) inMessage;
                    String bucketnm = txtmsg.getStringProperty("bucketnm");
                    String userid = txtmsg.getStringProperty("userid");
                    String itemnm = txtmsg.getText();

                    System.out.println("ITEMSTORE::ObjectMessageListener::Received Get Request::item=" + itemnm + ":bucket=" + bucketnm);

                    //Processing the get request
                    String server = txtmsg.getStringProperty("server");

                    Item item = em.find(Item.class, new Item.ItemId(itemnm, bucketnm, userid));

                    if (item != null && (item.getServers().indexOf(SERVER_LOCAL + ":") != -1 || item.getServers().indexOf(":" + SERVER_LOCAL) != -1)) {

                        //Sending reply
                        if (item.getBlob() != null) {
                            System.out.println("ITEMSTORE::MetadataRequestListener::Sending Reply to server=" + server);
                            try {
                                Context c = new InitialContext();
                                QueueConnectionFactory qcf = (QueueConnectionFactory) c.lookup("jms/" + server + "ConnectionFactory");
                                Queue requestQ = (Queue) c.lookup("jms/Queue");
                                QueueConnection qconn = qcf.createQueueConnection();
                                QueueSession qsession = qconn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
                                QueueSender sender = qsession.createSender(requestQ);
                                ObjectMessage objmsg = qsession.createObjectMessage(item);
                                objmsg.setStringProperty("type", item.getType());
                                objmsg.setStringProperty("method", "merge");
                                sender.send(objmsg);
                                qconn.close();
                            } catch (JMSException ex) {
                                Logger.getLogger(AppConfig.class.getName()).log(Level.SEVERE, null, ex);
                            } catch (NamingException ex) {
                                Logger.getLogger(AppConfig.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }

                    }

                }
            } else {
                System.out.println("Message of wrong type: " + inMessage.getClass().getName());
            }
        } catch (JMSException ex) {
            Logger.getLogger(ObjectMessageListener.class.getName()).log(Level.SEVERE, null, ex);
        }

        em.close();

    }
}
