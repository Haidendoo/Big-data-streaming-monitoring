package com.company.lakehouse.parser;

import com.company.lakehouse.model.ServerMetric;
import com.opencsv.CSVReader;
import java.io.StringReader;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class CsvMetricParser implements MetricParser {
    private static final long serialVersionUID = 1L;

    @Override
    public List<ServerMetric> parse(String content) throws Exception {
        List<ServerMetric> metrics = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new StringReader(content))) {
            List<String[]> lines = reader.readAll();
            for (int i = 0; i < lines.size(); i++) {
                String[] row = lines.get(i);
                // Skip header or empty rows
                if (row.length == 0 || (row.length > 0 && row[0].trim().isEmpty())) {
                    continue;
                }
                if (i == 0 && (row[0].equalsIgnoreCase("timestamp") || row[0].contains("timestamp"))) {
                    continue;
                }
                if (row.length < 7) {
                    continue;
                }
                try {
                    Instant instant = Instant.parse(row[0].trim());
                    Timestamp ts = Timestamp.from(instant);
                    String serverName = row[1].trim();
                    String ip = row[2].trim();
                    Double cpu = Double.parseDouble(row[3].trim());
                    Double ram = Double.parseDouble(row[4].trim());
                    Double disk = Double.parseDouble(row[5].trim());
                    Double io = Double.parseDouble(row[6].trim());
                    metrics.add(new ServerMetric(ts, serverName, ip, cpu, ram, disk, io));
                } catch (Exception e) {
                    // Log error and continue to parse other rows
                    System.err.println("Error parsing row: " + String.join(",", row) + " - " + e.getMessage());
                }
            }
        }
        return metrics;
    }
}
