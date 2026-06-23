package com.company.lakehouse.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class XmlServer implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("server_id")
    private Integer serverId;

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

    public Integer getServerId() {
        return serverId;
    }

    public void setServerId(Integer serverId) {
        this.serverId = serverId;
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
