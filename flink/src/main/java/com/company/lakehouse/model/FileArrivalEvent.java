package com.company.lakehouse.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class FileArrivalEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("file_path")
    private String filePath;

    @JsonProperty("file_name")
    private String fileName;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("format")
    private String format;

    public FileArrivalEvent() {}

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    @Override
    public String toString() {
        return "FileArrivalEvent{" +
                "filePath='" + filePath + '\'' +
                ", fileName='" + fileName + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", format='" + format + '\'' +
                '}';
    }
}
