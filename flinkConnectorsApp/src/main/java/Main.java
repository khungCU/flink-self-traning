import java.io.InputStream;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import deserializer.JsonCdcDeserializer;
import model.CdcEvent;
import reconciliation.TableRegistry;
import sinker.PGSinker;
import workflows.DBSyncWithSchemaWorkflow;

public class Main {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        try (InputStream input = Main.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new RuntimeException("Unable to find application.properties");
            }
            props.load(input);
        }

        // --- Infrastructure setup ---

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);

        TableRegistry registry = new TableRegistry();

        MySqlSource<CdcEvent> mysqlSource = MySqlSource.<CdcEvent>builder()
                .hostname(props.getProperty("mysql.hostname"))
                .port(Integer.parseInt(props.getProperty("mysql.port")))
                .databaseList(props.getProperty("mysql.database"))
                .tableList(registry.toMySqlTableList(props.getProperty("mysql.database")))
                .username(props.getProperty("mysql.username"))
                .password(props.getProperty("mysql.password"))
                .serverId("7100-7104")
                .serverTimeZone("UTC")
                .deserializer(new JsonCdcDeserializer())
                .startupOptions(StartupOptions.latest())
                .build();

        // Parallelism 1: MySQL binlog is a single stream, so only one reader makes sense
        DataStream<CdcEvent> cdcStream = env
                .fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1);

        String pgJdbcUrl = String.format("jdbc:postgresql://%s:%s/%s",
                props.getProperty("postgres.hostname"),
                props.getProperty("postgres.port"),
                props.getProperty("postgres.database"));

        PGSinker sink = new PGSinker(
                pgJdbcUrl,
                props.getProperty("postgres.username"),
                props.getProperty("postgres.password"),
                registry.getPrimaryKeys()
        );

        // --- Workflow assembly + execution ---

        new DBSyncWithSchemaWorkflow.Builder()
                .cdcSourceDataStream(cdcStream)
                .registry(registry)
                .sink(sink)
                .build()
                .execute();
    }
}
