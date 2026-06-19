package com.company.lakehouse.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class XmlServer implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("server_name")
    private String serverName;

    @JsonProperty("ip")
    private String ip;

    @JsonProperty("cpu")
    private Double cpu;

    @JsonProperty("ram")
    private Double ram;

    @JsonProperty("disk")
    private Double disk;

    @JsonProperty("io")
    private Double io;

    public XmlServer() {}

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Double getCpu() {
        return cpu;
    }

    public void setCpu(Double cpu) {
        this.cpu = cpu;
    }

    public Double getRam() {
        return ram;
    }

    public void setRam(Double ram) {
        this.ram = ram;
    }

    public Double getDisk() {
        return disk;
    }

    public void setDisk(Double disk) {
        this.disk = disk;
    }

    public Double getIo() {
        return io;
    }

    public void setIo(Double io) {
        this.io = io;
    }
}
