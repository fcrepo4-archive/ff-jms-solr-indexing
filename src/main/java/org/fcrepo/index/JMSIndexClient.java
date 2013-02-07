package org.fcrepo.index;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.NamingException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlValue;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.kahadb.util.ByteArrayInputStream;

public class JMSIndexClient implements Runnable {

    public static final String QUEUE_NAME = "TESTQUEUE-SOLR";

    private String brokerUrl;

    private ConnectionFactory jmsFac;

    private Session sess;

    private MessageConsumer consumer;

    private boolean stop = false;

    private HttpClient httpClient = new DefaultHttpClient();

    private URI solrUri = URI.create("http://localhost:8080/solr/update");

    private static Unmarshaller unmarshaller;

    private static Marshaller marshaller;

    public void run() {
        try {
            initService();
        } catch (NamingException e) {
            e.printStackTrace();
        } catch (JMSException e) {
            e.printStackTrace();
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        while (!stop) {
            try {
                TextMessage msg = (TextMessage) consumer.receive();
                String xml = msg.getText();
                System.out.println(xml);
                Entry e = (Entry) unmarshaller.unmarshal(new ByteArrayInputStream(xml.getBytes()));
                StringBuilder docBuilder = new StringBuilder("<add><doc>");
                docBuilder.append("<field name=\"id\">random-id-" + UUID.randomUUID() + "</field>");
                docBuilder.append("<field name=\"name\">" + e.id + "</field>");
                docBuilder.append("<field name=\"title\">" + e.title + "</field>");
                docBuilder.append("<field name=\"description\">" + e.summary + "</field>");
                docBuilder.append("<field name=\"content\">" + e.content + "</field>");
                docBuilder.append("</doc></add>");
                addToSolr(docBuilder.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void addToSolr(String doc) throws ClientProtocolException, IOException {
        HttpPost post = new HttpPost(solrUri);
        post.setEntity(new StringEntity(doc, ContentType.APPLICATION_XML));
        System.out.println(doc);
        HttpResponse resp = httpClient.execute(post);
        if (resp.getStatusLine().getStatusCode() != 200) {
            System.err.println("Unable to post document to Solr");
            System.err.println("Solr says: ");
            System.err.println(resp.getStatusLine().getStatusCode() + ": " + resp.getStatusLine().getReasonPhrase());
        }
    }

    private void initService() throws NamingException, JMSException, JAXBException {
        brokerUrl = ActiveMQConnection.DEFAULT_BROKER_URL;
        jmsFac = new ActiveMQConnectionFactory(brokerUrl);
        Connection conn = jmsFac.createConnection();
        conn.start();
        sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination dest = sess.createQueue(QUEUE_NAME);
        consumer = sess.createConsumer(dest);
        JAXBContext jaxbCtx = JAXBContext.newInstance(JMSIndexClient.Entry.class);
        unmarshaller = jaxbCtx.createUnmarshaller();
        marshaller = jaxbCtx.createMarshaller();
        marshaller.setProperty("com.sun.xml.internal.bind.namespacePrefixMapper", new JMSClientNameSpacePrefixMapper());
        marshaller.setProperty( Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE );
        

    }

    public void shutdown() {
        this.stop = true;
    }

    @XmlRootElement(name = "entry", namespace = "http://www.w3.org/2005/Atom")
    public static class Entry {
        @XmlElement(name = "id", namespace = "http://www.w3.org/2005/Atom")
        private String id;

        @XmlElement(name = "title", namespace = "http://www.w3.org/2005/Atom")
        private Title title;

        @XmlElement(name = "updated", namespace = "http://www.w3.org/2005/Atom")
        private String updated;

        @XmlElement(name = "author", namespace = "http://www.w3.org/2005/Atom")
        private Author author;

        @XmlElement(name = "summary", namespace = "http://www.w3.org/2005/Atom")
        private String summary;

        @XmlElement(name = "content", namespace = "http://www.w3.org/2005/Atom")
        private Content content;


        public void setId(String id) {
            this.id = id;
        }

        public void setTitle(Title title) {
            this.title = title;
        }

        public void setUpdated(String updated) {
            this.updated = updated;
        }

        public void setAuthor(Author author) {
            this.author = author;
        }


        public void setSummary(String summary) {
            this.summary = summary;
        }

        public void setContent(Content content) {
            this.content = content;
        }

    }

    public static class Author {
        @XmlElement(name = "name", namespace = "http://www.w3.org/2005/Atom")
        private String name;

        @XmlElement(name = "uri", namespace = "http://www.w3.org/2005/Atom")
        private URI uri;

        public void setName(String name) {
            this.name = name;
        }

        public void setUri(URI uri) {
            this.uri = uri;
        }

    }

    public static class Content {
        @XmlAttribute(name = "type")
        private String type;

        @XmlValue
        private String text;

        public void setType(String type) {
            this.type = type;
        }

        public void setText(String text) {
            this.text = text;
        }

    }
    
    public static class Title {
        @XmlAttribute(name="type")
        private String type;
        @XmlValue
        private String text;
        public void setType(String type) {
            this.type = type;
        }
        public void setText(String text) {
            this.text = text;
        }
        
    }

}
