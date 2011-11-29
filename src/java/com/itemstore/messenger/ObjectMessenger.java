package com.itemstore.messenger;

import com.itemstore.beans.entities.Item;
import com.itemstore.config.AppConfig;
import com.itemstore.types.Log;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.JMSException;
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

/**
 *
 * @author ashwanilabs
 */
public class ObjectMessenger implements Runnable {

    private static String SERVER_LOCAL = AppConfig.getLocalServer();
    private static String[] SERVER_LIST = AppConfig.getServerList();
    private Item item;
    private Log log;

    public ObjectMessenger(Item item, Log log) {
        this.item = item;
        this.log = log;
    }

    @Override
    public void run() {
        String tname = Thread.currentThread().getName();
        Context c = null;
        try {
            c = new InitialContext();
        } catch (NamingException ex) {
            Logger.getLogger(ObjectMessenger.class.getName()).log(Level.SEVERE, null, ex);
        }

        if (tname.equals("post")) {

            Set<String> serverSet = new HashSet<String>();
            String[] sList = item.getServers().split(":");
            serverSet.addAll(Arrays.asList(sList));
            serverSet.remove(SERVER_LOCAL);
            String[] serverList = new String[serverSet.size()];
            serverList = serverSet.toArray(serverList);

            for (int i = 0; i < serverList.length; i++) {
                String server = serverList[i];
                System.out.println("ITEMSTORE::ObjectMessager::Sending ObjectMessage::server=" + server + ":bucket=" + item.getBucketnm() + ":item=" + item.getItemnm());
                try {
                    QueueConnectionFactory qcf = (QueueConnectionFactory) c.lookup("jms/" + server + "ConnectionFactory");
                    Queue requestQ = (Queue) c.lookup("jms/Queue");
                    QueueConnection qconn = qcf.createQueueConnection();
                    QueueSession qsession = qconn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
                    QueueSender sender = qsession.createSender(requestQ);
                    ObjectMessage objmsg = qsession.createObjectMessage();
                    objmsg.setStringProperty("type", item.getType());
                    objmsg.setStringProperty("method", tname);
                    objmsg.setObject(item);
                    sender.send(objmsg);
                    qconn.close();
                } catch (JMSException ex) {
                    Logger.getLogger(ObjectMessenger.class.getName()).log(Level.SEVERE, null, ex);
                } catch (NamingException ex) {
                    Logger.getLogger(ObjectMessenger.class.getName()).log(Level.SEVERE, null, ex);
                }
            }


            //Update the other non-replicas about the new item in the system

            Set<String> serverSet2 = new HashSet<String>();
            serverSet2.addAll(Arrays.asList(SERVER_LIST));
            serverSet2.removeAll(Arrays.asList(sList));

            if (serverSet2.size() > 0) {
                String[] serverList2 = new String[serverSet2.size()];
                serverList = serverSet2.toArray(serverList2);

                for (int i = 0; i < serverList2.length; i++) {
                    String server = serverList2[i];
                    System.out.println("ITEMSTORE::ObjectMessager::Sending TextMessage::server=" + server + ":bucket=" + item.getBucketnm() + ":item=" + item.getItemnm());
                    try {
                        QueueConnectionFactory qcf2 = (QueueConnectionFactory) c.lookup("jms/" + server + "ConnectionFactory");
                        Queue requestQ2 = (Queue) c.lookup("jms/Queue");
                        QueueConnection qconn2 = qcf2.createQueueConnection();
                        QueueSession qsession2 = qconn2.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
                        QueueSender qsender2 = qsession2.createSender(requestQ2);
                        TextMessage txtmsg2 = qsession2.createTextMessage(item.getString());
                        txtmsg2.setStringProperty("type", item.getType());
                        txtmsg2.setStringProperty("method", tname);
                        qsender2.send(txtmsg2);
                        qconn2.close();
                    } catch (JMSException ex) {
                        Logger.getLogger(ObjectMessenger.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (NamingException ex) {
                        Logger.getLogger(ObjectMessenger.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }

            }
        } else if (tname.equals("put")) {

            Set<String> serverSet = new HashSet<String>();
            String[] sList = item.getServers().split(":");
            serverSet.addAll(Arrays.asList(sList));
            serverSet.remove(SERVER_LOCAL);
            String[] serverList = new String[serverSet.size()];
            serverList = serverSet.toArray(serverList);

            for (int i = 0; i < serverList.length; i++) {
                String server = serverList[i];
                System.out.println("ITEMSTORE::ObjectMessager::Sending ObjectMessage::server=" + server + ":bucket=" + item.getBucketnm() + ":item=" + item.getItemnm());
                try {
                    QueueConnectionFactory qcf = (QueueConnectionFactory) c.lookup("jms/" + server + "ConnectionFactory");
                    Queue requestQ = (Queue) c.lookup("jms/Queue");
                    QueueConnection qconn = qcf.createQueueConnection();
                    QueueSession qsession = qconn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
                    QueueSender sender = qsession.createSender(requestQ);
                    ObjectMessage objmsg = qsession.createObjectMessage();
                    objmsg.setStringProperty("item", item.getString());
                    objmsg.setStringProperty("type", item.getType());
                    objmsg.setStringProperty("method", tname);
                    objmsg.setObject(log);
                    sender.send(objmsg);
                    qconn.close();
                } catch (JMSException ex) {
                    Logger.getLogger(ObjectMessenger.class.getName()).log(Level.SEVERE, null, ex);
                } catch (NamingException ex) {
                    Logger.getLogger(ObjectMessenger.class.getName()).log(Level.SEVERE, null, ex);
                }
            }


            //Update the other non-replicas about the new item in the system

            Set<String> serverSet2 = new HashSet<String>();
            serverSet2.addAll(Arrays.asList(SERVER_LIST));
            serverSet2.removeAll(Arrays.asList(sList));

            if (serverSet2.size() > 0) {
                String[] serverList2 = new String[serverSet2.size()];
                serverList = serverSet2.toArray(serverList2);



                for (int i = 0; i < serverList2.length; i++) {
                    String server = serverList2[i];
                    System.out.println("ITEMSTORE::ObjectMessager::Sending ObjectMessage::server=" + server + ":bucket=" + item.getBucketnm() + ":item=" + item.getItemnm());
                    try {
                        QueueConnectionFactory qcf2 = (QueueConnectionFactory) c.lookup("jms/" + server + "ConnectionFactory");
                        Queue requestQ2 = (Queue) c.lookup("jms/Queue");
                        QueueConnection qconn2 = qcf2.createQueueConnection();
                        QueueSession qsession2 = qconn2.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
                        QueueSender qsender2 = qsession2.createSender(requestQ2);
                        TextMessage txtmsg2 = qsession2.createTextMessage(item.getString());
                        txtmsg2.setStringProperty("type", item.getType());
                        txtmsg2.setStringProperty("method", tname);
                        qsender2.send(txtmsg2);
                        qconn2.close();
                    } catch (JMSException ex) {
                        Logger.getLogger(ObjectMessenger.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (NamingException ex) {
                        Logger.getLogger(ObjectMessenger.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }

            }
        } else if (tname.equals("delete")) {

            for (int i = 0; i < SERVER_LIST.length; i++) {
                String server = SERVER_LIST[i];
                System.out.println("ITEMSTORE::ObjectMessager::Sending TextMessage::server=" + server + ":bucket=" + item.getBucketnm() + ":item=" + item.getItemnm());
                try {
                    QueueConnectionFactory qcf = (QueueConnectionFactory) c.lookup("jms/" + server + "ConnectionFactory");
                    Queue requestQ = (Queue) c.lookup("jms/Queue");
                    QueueConnection qconn = qcf.createQueueConnection();
                    QueueSession qsession = qconn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
                    QueueSender qsender = qsession.createSender(requestQ);
                    TextMessage txtmsg = qsession.createTextMessage(item.getItemnm());
                    txtmsg.setStringProperty("bucketnm", item.getBucketnm());
                    txtmsg.setStringProperty("userid", item.getUserid());
                    txtmsg.setStringProperty("type", item.getType());
                    txtmsg.setStringProperty("method", tname);
                    qsender.send(txtmsg);
                    qconn.close();
                } catch (JMSException ex) {
                    Logger.getLogger(ObjectMessenger.class.getName()).log(Level.SEVERE, null, ex);
                } catch (NamingException ex) {
                    Logger.getLogger(ObjectMessenger.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

        }
    }
}
