/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.itemstore.config;

import com.itemstore.beans.entities.Config;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;

/**
 *
 * @author ashwanilabs
 */
public class CheckPointer implements Runnable {

    @Override
    public void run() {
        EntityManager em = AppConfig.getEmf().createEntityManager();
        EntityTransaction et = em.getTransaction();
        while (true) {
            try {
                et.begin();
                Config config = em.find(Config.class, "CHECK_POINT");
                config.setParamval(Long.toString(System.currentTimeMillis()));
                em.merge(config);
                em.flush();
                et.commit();
                Thread.sleep(30000);
            } catch (InterruptedException ex) {
                Logger.getLogger(CheckPointer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }
}
