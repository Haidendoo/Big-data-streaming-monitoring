package com.company.lakehouse.parser;

import com.company.lakehouse.model.ServerMetric;
import com.company.lakehouse.model.XmlServer;
import com.company.lakehouse.model.XmlServers;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class XmlMetricParser implements MetricParser {
    private static final long serialVersionUID = 1L;
    private static final XmlMapper xmlMapper = new XmlMapper();

    @Override
    public List<ServerMetric> parse(String content) throws Exception {
        XmlServers xmlServers = xmlMapper.readValue(content, XmlServers.class);
        List<ServerMetric> metrics = new ArrayList<>();
        if (xmlServers != null && xmlServers.getServers() != null) {
            for (XmlServer s : xmlServers.getServers()) {
                Instant instant = Instant.parse(s.getTimestamp());
                Timestamp ts = Timestamp.from(instant);
                metrics.add(new ServerMetric(
                        ts,
                        s.getServerName(),
                        s.getIp(),
                        s.getCpu(),
                        s.getRam(),
                        s.getDisk(),
                        s.getIo()
                ));
            }
        }
        return metrics;
    }
}
