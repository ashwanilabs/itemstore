/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.itemstore.config;

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.JMSException;
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
public class ReplicaSync implements Runnable {

    private static final String SERVER_LOCAL = AppConfig.getLocalServer();

    @Override
    public void run() {
        //Query the nearest server for new updates if CHECK_POINT < LAST_MODIFIED
        System.out.println("ITEMSTORE::Getting Lost Updates");

        try {
            QueueConnection qconn = null;
            TextMessage txtmsg = null;
            QueueSender sender = null;
            QueueConnectionFactory qcf = null;
            Queue requestQ = null;
            QueueSession qsession = null;
            Context c = new InitialContext();
            String server = AppConfig.getServerList()[0];
            System.out.println("ITEMSTORE::ReplicaSync::Sending TextMessage::server=" + server);
            qcf = (QueueConnectionFactory) c.lookup("jms/" + server + "ConnectionFactory");
            requestQ = (Queue) c.lookup("jms/Queue");
            qconn = qcf.createQueueConnection();
            qsession = qconn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            sender = qsession.createSender(requestQ);
            txtmsg = qsession.createTextMessage(SERVER_LOCAL + ":" + AppConfig.getCheckPoint().getTime());
            txtmsg.setStringProperty("type", "mreq");
            sender.send(txtmsg);
            qconn.close();

        } catch (JMSException ex) {
            Logger.getLogger(ReplicaSync.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NamingException ex) {
            Logger.getLogger(ReplicaSync.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
