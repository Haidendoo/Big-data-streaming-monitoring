package com.company.lakehouse.parser;

import com.company.lakehouse.model.ServerMetric;
import java.io.Serializable;
import java.util.List;

public interface MetricParser extends Serializable {
    List<ServerMetric> parse(String content) throws Exception;
}
