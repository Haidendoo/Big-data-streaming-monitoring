package com.company.lakehouse.model;

import java.io.Serializable;
import java.sql.Timestamp;

public class ServerMetric implements Serializable {
    private static final long serialVersionUID = 1L;

    private Timestamp ts;
    private String serverName;
    private String ip;
    private Double cpuUtil;
    private Double ramUtil;
    private Double diskUtil;
    private Double ioStat;

    public ServerMetric() {}

    public ServerMetric(Timestamp ts, String serverName, String ip, Double cpuUtil, Double ramUtil, Double diskUtil, Double ioStat) {
        this.ts = ts;
        this.serverName = serverName;
        this.ip = ip;
        this.cpuUtil = cpuUtil;
        this.ramUtil = ramUtil;
        this.diskUtil = diskUtil;
        this.ioStat = ioStat;
    }

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
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

    public Double getCpuUtil() {
        return cpuUtil;
    }

    public void setCpuUtil(Double cpuUtil) {
        this.cpuUtil = cpuUtil;
    }

    public Double getRamUtil() {
        return ramUtil;
    }

    public void setRamUtil(Double ramUtil) {
        this.ramUtil = ramUtil;
    }

    public Double getDiskUtil() {
        return diskUtil;
    }

    public void setDiskUtil(Double diskUtil) {
        this.diskUtil = diskUtil;
    }

    public Double getIoStat() {
        return ioStat;
    }

    public void setIoStat(Double ioStat) {
        this.ioStat = ioStat;
    }

    @Override
    public String toString() {
        return "ServerMetric{" +
                "ts=" + ts +
                ", serverName='" + serverName + '\'' +
                ", ip='" + ip + '\'' +
                ", cpuUtil=" + cpuUtil +
                ", ramUtil=" + ramUtil +
                ", diskUtil=" + diskUtil +
                ", ioStat=" + ioStat +
                '}';
    }
}
