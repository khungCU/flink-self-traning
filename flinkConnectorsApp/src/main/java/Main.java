import java.io.InputStream;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import deserializer.JsonCdcDeserializer;
import model.CdcEvent;
import model.MessageNormalized;
import reconciliation.SchemaNormalizer;
import reconciliation.TableRegistry;
import sinker.PGSinker;

public class Main {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        try (InputStream input = Main.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new RuntimeException("Unable to find application.properties");
            }
            props.load(input);
        }

        MySqlSource<CdcEvent> mySQLSource = MySqlSource.<CdcEvent>builder()
            .hostname(props.getProperty("mysql.hostname"))
            .port(Integer.parseInt(props.getProperty("mysql.port")))
            .databaseList(props.getProperty("mysql.database"))
            .tableList(props.getProperty("mysql.table"))
            .username(props.getProperty("mysql.username"))
            .password(props.getProperty("mysql.password"))
            .serverId("7100-7104")
            .serverTimeZone("UTC")
            .deserializer(new JsonCdcDeserializer())
            .startupOptions(StartupOptions.latest())
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);

        // Set parallelism to 1 for CDC source - MySQL binlog is a single stream
        DataStream<CdcEvent> cdcStream = env.fromSource(mySQLSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
            .setParallelism(1);

        cdcStream.print();

        // Single source of truth for all per-table config
        TableRegistry registry = new TableRegistry();

        // Build Postgres JDBC URL from properties
        String pgJdbcUrl = String.format("jdbc:postgresql://%s:%s/%s",
            props.getProperty("postgres.hostname"),
            props.getProperty("postgres.port"),
            props.getProperty("postgres.database"));

        // Normalize CDC events into typed POJOs, routed via per-table side outputs
        SingleOutputStreamOperator<MessageNormalized> processed = cdcStream
            .keyBy(CdcEvent::getTable)
            .process(new SchemaNormalizer(registry));

        // Log schema-drift events (unknown tables) for alerting
        processed.getSideOutput(registry.getUnknownTag()).print("schema-drift");

        // Union known-table side outputs and sink to Postgres
        DataStream<MessageNormalized> normalizedStream = null;
        for (OutputTag<MessageNormalized> tag : registry.getAllKnownTags()) {
            DataStream<MessageNormalized> sideOutput = processed.getSideOutput(tag);
            normalizedStream = (normalizedStream == null) ? sideOutput : normalizedStream.union(sideOutput);
        }

        normalizedStream.sinkTo(new PGSinker(
            pgJdbcUrl,
            props.getProperty("postgres.username"),
            props.getProperty("postgres.password"),
            registry.getPrimaryKeys()
        ));

        env.execute("MySQL to Postgres CDC Mirror");
    }
}
