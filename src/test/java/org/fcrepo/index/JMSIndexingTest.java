package org.fcrepo.index;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.simpleframework.http.core.Container;
import org.simpleframework.http.core.ContainerServer;
import org.simpleframework.transport.Server;
import org.simpleframework.transport.connect.SocketConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:context-test.xml"})
public class JMSIndexingTest {
    
    private MessageProducer producer;
    private Session session;
    private JMSSolrIndexingService client;
    private BrokerService broker;
    private Container fedoraContainer;
    private Container solrContainer;
    private Connection connection;
    private Topic topic; 

    @Autowired
    @Qualifier("topicName")
    private String topicName;
    
    @Autowired
    private FedoraServiceRunner serviceRunner;
    
    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }
    
    public void setFedoraServiceRunner(FedoraServiceRunner serviceRunner){
        this.serviceRunner = serviceRunner;
    }

    @Before
    public  void setup() throws Exception {
        // setup a ActiveMQ Broker and MessageQueue
        broker = BrokerFactory.createBroker(URI.create("broker:tcp://localhost:61616"));
        broker.start();

        // create and start a connection to the solr service for usage in the message producer
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_BROKER_URL);
        connection = connectionFactory.createConnection();
        connection.start();

        // create the producer
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        System.out.println("TOPIC NAME: " + topicName);
        topic = session.createTopic(topicName);
        producer = session.createProducer(topic);

        // start the fedora http mock service
        fedoraContainer = new Fedora4Mock();
        Server fedoraServer = new ContainerServer(fedoraContainer);
        org.simpleframework.transport.connect.Connection fedoraConnection = new SocketConnection(fedoraServer);
        SocketAddress fedoraAddr = new InetSocketAddress(8080);
        fedoraConnection.connect(fedoraAddr);
        
        // start the Solr http mock service
        solrContainer = new SolrMock();
        Server solrServer = new ContainerServer(solrContainer);
        org.simpleframework.transport.connect.Connection solrConnection = new SocketConnection(solrServer);
        SocketAddress solrAddr = new InetSocketAddress(8081);
        solrConnection.connect(solrAddr);
        
        // run the fedora service runner in order to start the indexing service        
        Thread t = new Thread(serviceRunner);
        t.start();
        // wait for the client to come up
        Thread.sleep(1000);
    }

    @Test
    public void testUpdateQueue() throws Exception {
        // create a test message containing an Atom Pub XML representation
        TextMessage msg = session.createTextMessage();
        String xml = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream("atom_message.xml"));
        msg.setText(xml);
        producer.send(msg);
    }

    @After
    public void teardown() throws Exception {
        broker.stop();
        serviceRunner.shutdown();
    }
}
