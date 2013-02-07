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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.simpleframework.http.core.Container;
import org.simpleframework.http.core.ContainerServer;
import org.simpleframework.transport.Server;
import org.simpleframework.transport.connect.SocketConnection;

public class JMSIndexingTest {
    private static MessageProducer producer;

    private static Session session;

    private static JMSIndexClient client;

    private static BrokerService broker;
    
    private static Container fedoraContainer;
    
    private static Container solrContainer;
    
    private static Connection connection;
    
    private static Topic topic; 

    @BeforeClass
    public static void setup() throws Exception {
        // setup a ActiveMQ Broker and MessageQueue
        broker = BrokerFactory.createBroker(URI.create("broker:tcp://localhost:61616"));
        broker.start();

        // create and start a connection to the solr service for usage in the message producer
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_BROKER_URL);
        connection = connectionFactory.createConnection();
        connection.start();

        // create the producer
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        topic = session.createTopic(JMSIndexClient.TOPIC_NAME);
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
        
        // create the actual JMS client listening on the MessageQueue
        client = new JMSIndexClient();
        Thread t = new Thread(client);
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

    @AfterClass
    public static void teardown() throws Exception {
        broker.stop();
        client.shutdown();
    }
}
