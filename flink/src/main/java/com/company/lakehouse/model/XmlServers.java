package com.company.lakehouse.model;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import java.io.Serializable;
import java.util.List;

@JacksonXmlRootElement(localName = "servers")
public class XmlServers implements Serializable {
    private static final long serialVersionUID = 1L;

    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "server")
    private List<XmlServer> servers;

    public XmlServers() {}

    public List<XmlServer> getServers() {
        return servers;
    }

    public void setServers(List<XmlServer> servers) {
        this.servers = servers;
    }
}
