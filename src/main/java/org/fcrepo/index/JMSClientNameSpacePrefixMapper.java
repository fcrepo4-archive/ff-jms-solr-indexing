package org.fcrepo.index;

public class JMSClientNameSpacePrefixMapper extends com.sun.xml.bind.marshaller.NamespacePrefixMapper {
    @Override
    public String getPreferredPrefix(String ns, String suggestion, boolean reqPrefix) {
        return "";
    }
}
