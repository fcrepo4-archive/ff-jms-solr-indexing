package org.fcrepo.index;

import java.net.URI;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class JMSIndexingTest {
    private static MessageProducer producer;

    private static Session session;

    private static JMSIndexClient client;

    private static BrokerService broker;

    @BeforeClass
    public static void setup() throws Exception {
        // setup a ActiveMQ Broker and MessageQueue
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_BROKER_URL);
        broker = BrokerFactory.createBroker(URI.create("broker:tcp://localhost:61616"));
        broker.start();

        // create and start a connection to the solr service for usage in the message producer
        Connection connection = connectionFactory.createConnection();
        connection.start();

        // create the producer
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(JMSIndexClient.QUEUE_NAME);
        producer = session.createProducer(destination);

        // create the actual JMS client listening on the MessageQueue
        client = new JMSIndexClient();
        Thread t = new Thread(client);
        t.start();
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
