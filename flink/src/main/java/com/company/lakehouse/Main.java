package com.company.lakehouse;

import com.company.lakehouse.model.FileArrivalEvent;
import com.company.lakehouse.model.ServerMetric;
import com.company.lakehouse.parser.CsvMetricParser;
import com.company.lakehouse.parser.MetricParser;
import com.company.lakehouse.parser.XmlMetricParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.catalog.TableIdentifier;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main implements Serializable {
    private static final long serialVersionUID = 1L;

    public static void main(String[] args) throws Exception {
        // 1. Initialize Flink Execution Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Checkpointing is required for Iceberg to commit files and make data visible
        env.enableCheckpointing(60000); // 1 minute checkpoint interval
        
        System.out.println("Initializing Flink Pipeline for Server Monitoring...");

        // 2. Configure Kafka Source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka.streaming.svc.cluster.local:9092")
                .setTopics("file-arrival-events")
                .setGroupId("flink-lakehouse-monitoring")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka-File-Arrival-Events"
        );

        // 3. Deserialize Kafka JSON payload into FileArrivalEvent object
        DataStream<FileArrivalEvent> eventStream = kafkaStream.map(new MapFunction<String, FileArrivalEvent>() {
            private static final long serialVersionUID = 1L;
            private final ObjectMapper mapper = new ObjectMapper();

            @Override
            public FileArrivalEvent map(String value) throws Exception {
                return mapper.readValue(value, FileArrivalEvent.class);
            }
        }).name("Map-To-FileArrivalEvent");

        // 4. FlatMap: Download file from Minio S3, Parse CSV/XML, and emit ServerMetric records
        DataStream<ServerMetric> metricStream = eventStream.flatMap(new FlatMapFunction<FileArrivalEvent, ServerMetric>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void flatMap(FileArrivalEvent event, Collector<ServerMetric> out) throws Exception {
                try {
                    System.out.println("Processing event: " + event);
                    
                    // Setup Hadoop S3 FileSystem configuration inside Flink tasks
                    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
                    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
                    conf.set("fs.s3a.endpoint", "http://minio.lakehouse.svc.cluster.local:9000");
                    conf.set("fs.s3a.access.key", "admin");
                    conf.set("fs.s3a.secret.key", "password123");
                    conf.set("fs.s3a.path.style.access", "true");
                    conf.set("fs.s3a.connection.ssl.enabled", "false"); // Minio runs on HTTP
                    
                    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(event.getFilePath());
                    org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(conf);
                    
                    String content;
                    try (org.apache.hadoop.fs.FSDataInputStream in = fs.open(path)) {
                        content = org.apache.commons.io.IOUtils.toString(in, StandardCharsets.UTF_8);
                    }
                    
                    MetricParser parser;
                    if ("csv".equalsIgnoreCase(event.getFormat())) {
                        parser = new CsvMetricParser();
                    } else if ("xml".equalsIgnoreCase(event.getFormat())) {
                        parser = new XmlMetricParser();
                    } else {
                        System.err.println("Unsupported format: " + event.getFormat());
                        return;
                    }
                    
                    List<ServerMetric> metrics = parser.parse(content);
                    for (ServerMetric m : metrics) {
                        out.collect(m);
                    }
                    System.out.println("Successfully processed " + event.getFileName() + ". Emitted " + metrics.size() + " records.");
                } catch (Exception e) {
                    System.err.println("Failed to process file " + event.getFilePath() + ": " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }).name("Download-And-Parse-S3-Files");

        // 5. Map ServerMetric POJOs to Flink Row objects
        DataStream<Row> rowStream = metricStream.map(new MapFunction<ServerMetric, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row map(ServerMetric m) throws Exception {
                Row row = new Row(7);
                // ts TIMESTAMP(6) in Trino maps to LocalDateTime inside Flink Table API
                row.setField(0, m.getTs().toLocalDateTime());
                row.setField(1, m.getServerName());
                row.setField(2, m.getIp());
                row.setField(3, m.getCpuUtil());
                row.setField(4, m.getRamUtil());
                row.setField(5, m.getDiskUtil());
                row.setField(6, m.getIoStat());
                return row;
            }
        }).name("Map-To-Flink-Row");

        // 6. Define Iceberg Catalog loader
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.hive.HiveCatalog");
        catalogProperties.put(CatalogProperties.URI, "thrift://hive-metastore.lakehouse.svc.cluster.local:9083");
        catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3a://landing-zone/lakehouse/");
        
        // Iceberg AWS S3 file access settings
        catalogProperties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        catalogProperties.put("s3.endpoint", "http://minio.lakehouse.svc.cluster.local:9000");
        catalogProperties.put("s3.access-key-id", "admin");
        catalogProperties.put("s3.secret-access-key", "password123");
        catalogProperties.put("s3.path-style-access", "true");

        CatalogLoader catalogLoader = CatalogLoader.hive(
                "hive_catalog",
                new org.apache.hadoop.conf.Configuration(),
                catalogProperties
        );

        TableIdentifier tableId = TableIdentifier.of("monitoring", "server_metrics");
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);

        // 7. Define the table schema matching the target Iceberg table
        TableSchema tableSchema = TableSchema.builder()
                .field("ts", DataTypes.TIMESTAMP(6))
                .field("server_name", DataTypes.STRING())
                .field("ip", DataTypes.STRING())
                .field("cpu_util", DataTypes.DOUBLE())
                .field("ram_util", DataTypes.DOUBLE())
                .field("disk_util", DataTypes.DOUBLE())
                .field("io_stat", DataTypes.DOUBLE())
                .build();

        // 8. Sink Flink stream to Iceberg Table
        FlinkSink.forRow(rowStream, tableSchema)
                .tableLoader(tableLoader)
                .append();

        // 9. Execute Pipeline
        env.execute("Flink-Server-Metrics-Ingestion-To-Iceberg");
    }
}
