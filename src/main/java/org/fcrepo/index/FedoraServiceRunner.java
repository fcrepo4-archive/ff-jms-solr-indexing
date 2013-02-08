package org.fcrepo.index;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class FedoraServiceRunner implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(FedoraServiceRunner.class);
    
    public static final int SERVICE_STATE_STARTING = 1;

    public static final int SERVICE_STATE_RUNNING = 2;

    public static final int SERRVICE_STATE_ERROR = 3;

    private List<FedoraJMSService> services;

    private boolean shutdown = false;

    public void setServices(List<FedoraJMSService> services) {
        this.services = services;
    }

    @Override
    public void run() {
        final Map<String, Integer> serviceStates = new ConcurrentHashMap<String, Integer>();
        for (final FedoraJMSService s : services) {
            serviceStates.put(s.getClass().getName(), SERVICE_STATE_STARTING);
            Runnable r = new Runnable() {
                public void run() {
                    if (s.startService()) {
                        serviceStates.put(s.getClass().getName(), SERVICE_STATE_RUNNING);
                    } else {
                        serviceStates.put(s.getClass().getName(), SERRVICE_STATE_ERROR);
                    }
                }
            };
            Thread t = new Thread(r);
            t.start();
            LOG.info("Waiting for services....");
            boolean servicesStarting = true;
            while (servicesStarting) {
                servicesStarting = false;
                for (Entry<String, Integer> e : serviceStates.entrySet()) {
                    if (e.getValue() == SERVICE_STATE_STARTING) {
                        servicesStarting = true;
                    }
                }
            }
        }
        for (Entry<String, Integer> e : serviceStates.entrySet()) {
            if (e.getValue() == SERRVICE_STATE_ERROR) {
                LOG.error("Service '" + e.getKey() + "' did not start sucessfully. Fix this and restart the service runner");
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
        for (FedoraJMSService s : services) {
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
    }
}
