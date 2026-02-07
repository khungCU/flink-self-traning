import java.io.InputStream;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import deserializer.ShipmentDebeziumDeserializer;
import model.ShipmentCdcEvent;

public class Main {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        try (InputStream input = Main.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new RuntimeException("Unable to find application.properties");
            }
            props.load(input);
        }

        MySqlSource<ShipmentCdcEvent> mySQLSource = MySqlSource.<ShipmentCdcEvent>builder()
            .hostname(props.getProperty("mysql.hostname"))
            .port(Integer.parseInt(props.getProperty("mysql.port")))
            .databaseList(props.getProperty("mysql.database"))
            .tableList(props.getProperty("mysql.table"))
            .username(props.getProperty("mysql.username"))
            .password(props.getProperty("mysql.password"))
            .serverId("6100-6104")
            .serverTimeZone("UTC")
            .deserializer(new ShipmentDebeziumDeserializer())
            .startupOptions(StartupOptions.latest())
            .build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.enableCheckpointing(3000);
        
        // Set parallelism to 1 for CDC source - MySQL binlog is a single stream
        DataStream<ShipmentCdcEvent> shipmentStream = env.fromSource(mySQLSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
            .setParallelism(1);
        
        // // filter only update and delete event
        DataStream<ShipmentCdcEvent> shipmentFilteredStream = shipmentStream.filter(event -> event.getOp().equals("d")  || event.getOp().equals("u"));

        // map to transform output event
        DataStream<String> outputStream = shipmentFilteredStream.map(event -> {
            return switch (event.getOp()) {
                case "d" -> "Shipment ID: " + event.getBefore().getShipmentId() + " has been deleted";
                case "u" -> "Shipment ID: " + event.getBefore().getShipmentId() + " has been updated";
                default -> "Unknown Event has been detected";
            };
        });

        // Print the transformed output events
        outputStream.print();

        env.execute("Print MySQL Snapshot + Binlog");

    }
}

