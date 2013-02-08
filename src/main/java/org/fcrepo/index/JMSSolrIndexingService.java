package org.fcrepo.index;

import java.io.IOException;
import java.text.ParseException;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import javax.naming.NamingException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.kahadb.util.ByteArrayInputStream;
import org.fcrepo.messaging.client.AbstractJMSClient;
import org.fcrepo.service.FedoraService;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

public class JMSSolrIndexingService extends AbstractJMSClient implements FedoraService, InitializingBean {

    private static final Logger LOG = LoggerFactory.getLogger(JMSSolrIndexingService.class);

    private boolean shutdown = false;

    private HttpClient httpClient = new DefaultHttpClient();

    private static Unmarshaller unmarshaller;

    private static Marshaller marshaller;

    private boolean serviceRunning = false;

    // spring injected values
    private String solrUrl;

    private String modeShapeUrl;

    public JMSSolrIndexingService(String brokerUrl, String topicName) throws JMSException {
        super(brokerUrl, topicName);
    }

    public void setSolrUrl(String solrUrl) {
        this.solrUrl = solrUrl;
    }

    public void setModeShapeUrl(String modeShapeUrl) {
        this.modeShapeUrl = modeShapeUrl;
    }

    @Override
    public synchronized boolean isRunning() {
        return serviceRunning;
    }
    
    @Override
    public void afterPropertiesSet() throws Exception {
        if (modeShapeUrl == null) {
            throw new IllegalArgumentException("Nodeshape url can not be null. Please check your spring configuration");
        }
    }

    public Object call() throws JAXBException, JMSException, NamingException {
        initService();
        serviceRunning = true;
        while (!shutdown) {
            try {
                TextMessage msg = (TextMessage) this.getNextMessage();
                String xml = msg.getText();
                // get the id from the message in order to fetch more metadata
                // from the repo
                Entry e = (Entry) unmarshaller.unmarshal(new ByteArrayInputStream(xml.getBytes()));
                ObjectProfile profile = fetchProfile(e.category.term);
                addToSolr(profile);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private ObjectProfile fetchProfile(String id) throws IOException, IllegalStateException, JAXBException {
        HttpGet get = new HttpGet(modeShapeUrl + "/rest/objects/" + id);
        LOG.debug("fetching from URL: " + get.getURI().toASCIIString());
        HttpResponse resp = httpClient.execute(get);
        String profile = IOUtils.toString(resp.getEntity().getContent());
        return (ObjectProfile) unmarshaller.unmarshal(new ByteArrayInputStream(profile.getBytes()));
    }

    private void addToSolr(ObjectProfile profile) throws ClientProtocolException, IOException, ParseException {
        // create the XML request
        String doc = createXmlRequest(profile);
        HttpPost post = new HttpPost(solrUrl);
        post.setEntity(new StringEntity(doc, ContentType.APPLICATION_XML));
        HttpResponse resp = httpClient.execute(post);
        if (resp.getStatusLine().getStatusCode() != 200) {
            LOG.error("Unable to post document to Solr");
            LOG.error("Solr says: ");
            LOG.error(resp.getStatusLine().getStatusCode() + ": " + resp.getStatusLine().getReasonPhrase());
        }
    }

    private String createXmlRequest(ObjectProfile profile) {
        // translate lastModDate to UTC for Solr
        DateTimeFormatter df = ISODateTimeFormat.dateTime();
        DateTime dt = df.parseDateTime(profile.objLastModDate);
        DateTime dtUtc = new DateTime(dt.getMillis(), DateTimeZone.UTC);

        StringBuilder xmlBuilder = new StringBuilder("<add><doc>");
        xmlBuilder.append("<field name=\"id\">" + profile.pid + "</field>");
        xmlBuilder.append("<field name=\"title\">" + profile.objLabel + "</field>");
        xmlBuilder.append("<field name=\"author\">" + profile.objOwnerId + "</field>");
        xmlBuilder.append("<field name=\"last_modified\">" + df.print(dtUtc) + "</field>");
        xmlBuilder.append("</doc></add>");
        return xmlBuilder.toString();
    }

    private void initService() throws NamingException, JMSException, JAXBException {
        JAXBContext jaxbCtx = JAXBContext.newInstance(Entry.class, ObjectProfile.class);
        unmarshaller = jaxbCtx.createUnmarshaller();
        marshaller = jaxbCtx.createMarshaller();
        marshaller.setProperty("com.sun.xml.bind.namespacePrefixMapper", new JMSClientNameSpacePrefixMapper());
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
    }

    public void stopService() {
        this.shutdown = true;
    }
}
