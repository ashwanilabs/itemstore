/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.itemstore.messenger;

import com.itemstore.beans.entities.Bucket;
import com.itemstore.config.AppConfig;
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
public class BucketMessenger implements Runnable {

    private static String[] SERVER_LIST = AppConfig.getServerList();
    private Bucket bucket;

    public BucketMessenger(Bucket bucket) {
        this.bucket = bucket;
    }

    @Override
    public void run() {
        String tname = Thread.currentThread().getName();
        Context c = null;
        try {
            c = new InitialContext();
        } catch (NamingException ex) {
            Logger.getLogger(BucketMessenger.class.getName()).log(Level.SEVERE, null, ex);
        }

        if (tname.equals("post") || tname.equals("put")) {

            for (int i = 0; i < SERVER_LIST.length; i++) {
                String server = SERVER_LIST[i];
                System.out.println("ITEMSTORE::BucketMessager::Sending ObjectMessage::server=" + server + ":bucket=" + bucket.getBucketnm());
                try {
                    QueueConnectionFactory qcf = (QueueConnectionFactory) c.lookup("jms/" + server + "ConnectionFactory");
                    Queue requestQ = (Queue) c.lookup("jms/Queue");
                    QueueConnection qconn = qcf.createQueueConnection();
                    QueueSession qsession = qconn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
                    QueueSender qsender = qsession.createSender(requestQ);
                    ObjectMessage objmsg = qsession.createObjectMessage(bucket);
                    objmsg.setStringProperty("method", tname);
                    objmsg.setStringProperty("type", "b");
                    qsender.send(objmsg);
                    qconn.close();
                } catch (JMSException ex) {
                    Logger.getLogger(BucketMessenger.class.getName()).log(Level.SEVERE, null, ex);
                } catch (NamingException ex) {
                    Logger.getLogger(BucketMessenger.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

        } else if (tname.equals("delete")) {

            for (int i = 0; i < SERVER_LIST.length; i++) {
                String server = SERVER_LIST[i];
                System.out.println("ITEMSTORE::BucketMessager::Sending ObjectMessage::server=" + server + ":bucket=" + bucket.getBucketnm());
                try {
                    QueueConnectionFactory qcf = (QueueConnectionFactory) c.lookup("jms/" + server + "ConnectionFactory");
                    Queue requestQ = (Queue) c.lookup("jms/Queue");
                    QueueConnection qconn = qcf.createQueueConnection();
                    QueueSession qsession = qconn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
                    QueueSender sender = qsession.createSender(requestQ);
                    TextMessage txtmsg = qsession.createTextMessage(bucket.getBucketnm());
                    txtmsg.setStringProperty("user", bucket.getUserid());
                    txtmsg.setStringProperty("method", tname);
                    txtmsg.setStringProperty("type", "b");
                    sender.send(txtmsg);
                    qconn.close();
                } catch (JMSException ex) {
                    Logger.getLogger(BucketMessenger.class.getName()).log(Level.SEVERE, null, ex);
                } catch (NamingException ex) {
                    Logger.getLogger(BucketMessenger.class.getName()).log(Level.SEVERE, null, ex);
                }

            }
        }
    }
}
