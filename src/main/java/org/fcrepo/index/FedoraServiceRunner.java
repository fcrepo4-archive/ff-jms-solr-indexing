package org.fcrepo.index;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class FedoraServiceRunner implements Runnable {
    private List<FedoraJMSService> services;
    private boolean shutdown = false;
    
    public void setServices(List<FedoraJMSService> services) {
        this.services = services;
    }
    
    @Override
    public void run() {
        final Map<String,Boolean> serviceStates = new ConcurrentHashMap<String, Boolean>();
        for (final FedoraJMSService s: services) {
            Runnable r = new Runnable() {
                public void run() {
                    serviceStates.put(s.getClass().getName(), s.startService());
                }
            };
            Thread t = new Thread(r);
            t.start();
            try {
                System.out.print("Waiting for services....");
                Thread.sleep(1000);
                System.out.println("done.");
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }
        for (Entry<String, Boolean> e : serviceStates.entrySet()) {
            System.out.println("Map has entry: " + e.getKey() + ": " + e.getValue());
            if (e.getValue() == false) {
                System.err.println("Service '" + e.getKey() + "' did not start sucessfully. Fix this and restart the service runner");
                shutdown = true;
            }
        }
        while (!shutdown) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        for (FedoraJMSService s: services) {
            s.stopService();
        }
    }

    public void shutdown() {
        this.shutdown = true;
    }
    
    public static void main(String[] args) {
        ApplicationContext ctx = new ClassPathXmlApplicationContext("context.xml");
        FedoraServiceRunner serviceRunner = (FedoraServiceRunner) ctx.getBean("fedoraServiceRunner");
        Thread t = new Thread(serviceRunner);
        t.start();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
