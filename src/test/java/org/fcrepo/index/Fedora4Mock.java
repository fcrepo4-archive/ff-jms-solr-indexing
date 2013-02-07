package org.fcrepo.index;

import java.io.IOException;
import java.io.OutputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.commons.io.IOUtils;
import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.simpleframework.http.core.Container;

public class Fedora4Mock implements Container{
    
    private Marshaller marshaller;
    
    public Fedora4Mock(){
        JAXBContext ctx;
        try {
            ctx = JAXBContext.newInstance(ObjectProfile.class);
            marshaller = ctx.createMarshaller();
        } catch (JAXBException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void handle(Request req, Response resp) {
        System.out.println("REQUEST: " + req.getTarget());
        ObjectProfile profile = new ObjectProfile();
        profile.objLabel = "Mocked test object";
        profile.objOwnerId = "MockUser";
        profile.objLastModDate = "2013-02-07T22:00:09.358+01:00";
        profile.pid = req.getTarget().substring(req.getTarget().lastIndexOf('/') + 1);
        OutputStream sink;
        try {
            sink = resp.getOutputStream();
            marshaller.marshal(profile,sink);
            IOUtils.closeQuietly(sink);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
