/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.itemstore.config;

import com.itemstore.beans.entities.Config;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

/**
 * Web application lifecycle listener.
 * @author ashwanilabs
 */
@WebListener()
public class AppConfig implements ServletContextListener {

    private static EntityManagerFactory emf;
    private static int serverNos;
    private static String localServer;
    private static String[] serverList;
    private static String serverListString;
    private static String storeLoc;
    private static int replicaCount;
    private static Date checkPoint;
    //private static Date lastModified;

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        String networkGraph = "";
        System.out.println("ITEMSTORE::Initializing");

        //Query Table CONFIG
        emf = Persistence.createEntityManagerFactory("itemstorePU");
        EntityManager em = emf.createEntityManager();
        Query q = em.createQuery("SELECT c FROM CONFIG c");
        List<Config> configList = q.getResultList();

        //Initialize Application Parameters
        for (Config c : configList) {
            if (c.getParamnm().equals("STORE_LOC")) {
                storeLoc = c.getParamval();
                System.out.println("ITEMSTORE::Param STORE_LOC = " + storeLoc);
            } else if (c.getParamnm().equals("SERVER_LOCAL")) {
                localServer = c.getParamval();
                System.out.println("ITEMSTORE::Param SERVER_LOCAL = " + localServer);
            } else if (c.getParamnm().equals("SERVER_NOS")) {
                serverNos = Integer.parseInt(c.getParamval());
                System.out.println("ITEMSTORE::Param SERVER_NOS = " + serverNos);
            } else if (c.getParamnm().equals("REPLICA_COUNT")) {
                replicaCount = Integer.parseInt(c.getParamval());
                System.out.println("ITEMSTORE::Param REPLICA_COUNT = " + replicaCount);
            } else if (c.getParamnm().equals("SERVER_LIST")) {
                serverListString = c.getParamval();
                System.out.println("ITEMSTORE::Param SERVER_LIST = " + serverListString);
            } else if (c.getParamnm().equals("NETWORK_GRAPH")) {
                networkGraph = c.getParamval();
                System.out.println("ITEMSTORE::Param NETWORK_GRAPH = " + networkGraph);
            } /*else if (c.getParamnm().equals("LAST_MODIFIED")) {
            long timeMillis = Long.parseLong(c.getParamval());
            GregorianCalendar time = new GregorianCalendar();
            time.setTimeInMillis(timeMillis);
            lastModified = time.getTime();
            System.out.println("ITEMSTORE::Param LAST_MODIFIED = " + lastModified);
            }*/ else if (c.getParamnm().equals("CHECK_POINT")) {
                long timeMillis = Long.parseLong(c.getParamval());
                GregorianCalendar time = new GregorianCalendar();
                time.setTimeInMillis(timeMillis);
                checkPoint = time.getTime();
                System.out.println("ITEMSTORE::Param CHECK_POINT = " + checkPoint);
            }
        }
        if (serverListString != null && localServer != null) {
            String[] servers = serverListString.split(":");
            int index = -1;
            for (String server : servers) {
                index++;
                if (server.equals(localServer)) {
                    break;
                }
            }
            String[] graph = networkGraph.split(":");
            String[] list = graph[index].split(" ");
            ArrayList<String> arr = new ArrayList<String>(serverNos);
            for (int i = 0; i < serverNos; i++) {
                arr.add(list[i] + ":" + servers[i]);
            }
            Collections.sort(arr);
            for (int i = 0; i < serverNos; i++) {
                String temp = arr.get(i);
                temp = temp.substring(temp.indexOf(':') + 1);
                arr.set(i, temp);
            }
            arr.remove(0);
            serverList = arr.toArray(new String[arr.size()]);
        }

        //Start the Backgroung Process for Updating the Store
        if (checkPoint.getTime() > 1) {
            Thread t = new Thread(new ReplicaSync());
            t.start();
        } else {
            Thread t = new Thread(new CheckPointer());
            t.start();
        }

        System.out.println("ITEMSTORE::Ready");
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        System.out.println("ITEMSTORE::Shutdown");
    }

    public static String getRepServers(int num) {
        String result = "";
        int tot = serverList.length;
        int q = tot / num;
        for (int i = 0; i < num; i++) {
            result += ":" + serverList[i * q];
        }
        return result.substring(1);
    }

    public static String getServerListString() {
        return serverListString;
    }

    public static String[] getServerList() {
        return serverList;
    }

    public static String getLocalServer() {
        return localServer;
    }

    public static int getServerNos() {
        return serverNos;
    }

    public static String getStoreLoc() {
        return storeLoc;
    }

    public static int getReplicaCount() {
        return replicaCount;
    }

    public static EntityManagerFactory getEmf() {
        return emf;
    }

    public static Date getCheckPoint() {
        return checkPoint;
    }
}
