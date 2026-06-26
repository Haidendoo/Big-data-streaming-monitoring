package com.company.lakehouse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class CdcMain implements Serializable {
    private static final long serialVersionUID = 1L;

    public static void main(String[] args) throws Exception {
        // Set Thread Context ClassLoader to user code classloader
        Thread.currentThread().setContextClassLoader(CdcMain.class.getClassLoader());

        Configuration conf = new Configuration();
        java.util.List<String> classpaths = java.util.Arrays.asList(
                "file:///opt/flink/usrlib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar",
                "file:///opt/flink/usrlib/flink-sql-connector-hive-3.1.3_2.12-1.18.1.jar"
        );
        conf.set(org.apache.flink.configuration.PipelineOptions.CLASSPATHS, classpaths);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(10000); // 10s checkpointing

        System.out.println("Initializing Programmatic Flink CDC from PostgreSQL to Iceberg...");

        // 1. Configure PostgreSQL CDC Source using DataStream API
        com.ververica.cdc.debezium.DebeziumSourceFunction<String> postgresSource = PostgreSQLSource.<String>builder()
                .hostname("dim-data-postgresql.lakehouse.svc.cluster.local")
                .port(5432)
                .database("dim_data")
                .schemaList("public")
                .tableList("public.server_config")
                .username("dim_user")
                .password("dim_pass")
                .decodingPluginName("pgoutput")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStream<String> cdcStream = env.addSource(postgresSource, "Postgres-CDC-Source");

        // 2. Map Debezium JSON change events to Flink Row objects
        DataStream<Row> rowStream = cdcStream.flatMap(new FlatMapFunction<String, Row>() {
            private static final long serialVersionUID = 1L;
            private transient ObjectMapper mapper;

            @Override
            public void flatMap(String value, Collector<Row> out) throws Exception {
                if (mapper == null) {
                    mapper = new ObjectMapper();
                }
                
                System.out.println("CDC Event: " + value);
                JsonNode root = mapper.readTree(value);
                String op = root.get("op").asText(); // "c" = create, "u" = update, "d" = delete, "r" = read
                
                JsonNode dataNode = null;
                RowKind rowKind = RowKind.INSERT;
                
                if ("d".equals(op)) {
                    dataNode = root.get("before");
                    rowKind = RowKind.DELETE;
                } else {
                    dataNode = root.get("after");
                    if ("u".equals(op)) {
                        rowKind = RowKind.UPDATE_AFTER;
                    }
                }
                
                if (dataNode != null && !dataNode.isNull()) {
                    int serverId = dataNode.get("server_id").asInt();
                    String serverName = dataNode.get("server_name").asText();
                    String ip = dataNode.get("ip").asText();
                    String province = dataNode.get("province").asText();
                    String station = dataNode.get("station").asText();
                    
                    Row row = Row.withPositions(rowKind, 5);
                    row.setField(0, serverId);
                    row.setField(1, serverName);
                    row.setField(2, ip);
                    row.setField(3, province);
                    row.setField(4, station);
                    out.collect(row);
                }
            }
        }).name("Map-CDC-Json-To-Row");

        // 3. Configure Iceberg Catalog loader
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.hive.HiveCatalog");
        catalogProperties.put(CatalogProperties.URI, "thrift://hive-metastore.lakehouse.svc.cluster.local:9083");
        catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3a://lakehouse/lakehouse/");
        catalogProperties.put("io-impl", "org.apache.iceberg.hadoop.HadoopFileIO");

        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.endpoint", "minio.lakehouse.svc.cluster.local:9000");
        hadoopConf.set("fs.s3a.access.key", "admin");
        hadoopConf.set("fs.s3a.secret.key", "password123");
        hadoopConf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        hadoopConf.set("fs.s3a.path-style-access", "true");
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConf.set("fs.s3a.connection.ssl.enabled", "false");
        hadoopConf.set("fs.s3a.connection.timeout", "5000");
        hadoopConf.set("fs.s3a.attempts.maximum", "3");
        hadoopConf.set("fs.s3a.endpoint.region", "us-east-1");

        CatalogLoader catalogLoader = CatalogLoader.hive(
                "hive_catalog",
                hadoopConf,
                catalogProperties
        );

        TableIdentifier tableId = TableIdentifier.of("monitoring", "server_config");
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);

        // 4. Define Table Schema matching server_config
        TableSchema tableSchema = TableSchema.builder()
                .field("server_id", DataTypes.INT())
                .field("server_name", DataTypes.STRING())
                .field("ip", DataTypes.STRING())
                .field("province", DataTypes.STRING())
                .field("station", DataTypes.STRING())
                .build();

        // 5. Sink to Iceberg
        FlinkSink.forRow(rowStream, tableSchema)
                .tableLoader(tableLoader)
                .overwrite(false)
                .append();

        env.execute("Flink-CDC-Postgres-Config-To-Iceberg");
    }
}
