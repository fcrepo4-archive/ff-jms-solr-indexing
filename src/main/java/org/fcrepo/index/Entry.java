package org.fcrepo.index;

import java.net.URI;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlValue;

@XmlRootElement(name = "entry", namespace = "http://www.w3.org/2005/Atom")
public class Entry {
    @XmlElement(name = "id", namespace = "http://www.w3.org/2005/Atom")
    String id;

    @XmlElement(name = "title", namespace = "http://www.w3.org/2005/Atom")
    Title title;

    @XmlElement(name = "updated", namespace = "http://www.w3.org/2005/Atom")
    String updated;

    @XmlElement(name = "author", namespace = "http://www.w3.org/2005/Atom")
    Author author;

    @XmlElement(name = "summary", namespace = "http://www.w3.org/2005/Atom")
    String summary;

    @XmlElement(name = "content", namespace = "http://www.w3.org/2005/Atom")
    Content content;

    @XmlElement(name = "category", namespace = "http://www.w3.org/2005/Atom")
    Category category;

    public void setCategory(Category category) {
        this.category = category;
    }

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

    public static class Author {
        @XmlElement(name = "name", namespace = "http://www.w3.org/2005/Atom")
        String name;

        @XmlElement(name = "uri", namespace = "http://www.w3.org/2005/Atom")
        URI uri;

        public void setName(String name) {
            this.name = name;
        }

        public void setUri(URI uri) {
            this.uri = uri;
        }

    }

    public static class Content {
        @XmlAttribute(name = "type")
        String type;

        @XmlValue
        String text;

        public void setType(String type) {
            this.type = type;
        }

        public void setText(String text) {
            this.text = text;
        }

    }

    public static class Title {
        @XmlAttribute(name = "type")
        String type;

        @XmlValue
        String text;

        public void setType(String type) {
            this.type = type;
        }

        public void setText(String text) {
            this.text = text;
        }
    }

    public static class Category {
        @XmlAttribute(name = "term")
        String term;

        @XmlAttribute(name = "scheme")
        String scheme;

        @XmlAttribute(name = "label")
        String label;

        public void setTerm(String term) {
            this.term = term;
        }

        public void setScheme(String scheme) {
            this.scheme = scheme;
        }

        public void setLabel(String label) {
            this.label = label;
        }
    }

}
