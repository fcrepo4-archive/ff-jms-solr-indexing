package org.fcrepo.index;

import java.io.IOException;

import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.simpleframework.http.core.Container;

public class SolrMock implements Container{
    @Override
    public void handle(Request req, Response resp) {
        try {
            System.out.println("SOLR mock recv: " + req.getContent());
            resp.setCode(200);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
