package deserializer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import model.CdcEvent;

/**
 * Generic Debezium deserializer that works with ANY table.
 *
 * Instead of mapping Struct fields to a specific POJO (like ShipmentDebeziumDeserializer does),
 * this converts the before/after Structs into JSON strings. This means it can handle CDC events
 * from multiple tables with different schemas in the same source.
 *
 * Usage:
 * <pre>
 * MySqlSource<CdcEvent> source = MySqlSource.<CdcEvent>builder()
 *     .tableList("db_1.shipments", "db_1.orders")   // multiple tables!
 *     .deserializer(new JsonCdcDeserializer())
 *     .build();
 * </pre>
 *
 * Downstream, use the table name to route events:
 * <pre>
 * stream.filter(e -> "shipments".equals(e.getTable()))  // filter by table
 *       .map(e -> objectMapper.readValue(e.getAfter(), Shipment.class))  // parse JSON to POJO
 * </pre>
 */
public class JsonCdcDeserializer implements DebeziumDeserializationSchema<CdcEvent> {
    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<CdcEvent> out) throws Exception {
        Struct value = (Struct) record.value();
        if (value == null) {
            return;
        }

        CdcEvent event = new CdcEvent();

        // Extract operation type
        event.setOp(value.getString("op"));

        // Extract timestamp
        Long tsMs = value.getInt64("ts_ms");
        if (tsMs != null) {
            event.setTsMs(tsMs);
        }

        // Extract source metadata (database + table name)
        Struct source = value.getStruct("source");
        if (source != null) {
            event.setDatabase(source.getString("db"));
            event.setTable(source.getString("table"));
        }

        // Convert before/after Structs to JSON strings (works for any table schema)
        Struct before = value.getStruct("before");
        if (before != null) {
            event.setBefore(structToJson(before));
        }

        Struct after = value.getStruct("after");
        if (after != null) {
            event.setAfter(structToJson(after));
        }

        out.collect(event);
    }

    /**
     * Converts any Kafka Connect Struct to a JSON string by iterating over its schema fields.
     * This is table-agnostic - it doesn't need to know the column names in advance.
     */
    private String structToJson(Struct struct) throws Exception {
        ObjectNode node = getObjectMapper().createObjectNode();

        for (Field field : struct.schema().fields()) {
            String name = field.name();
            Object val = struct.get(field);

            switch (val) {
                case null        -> node.putNull(name);
                case Integer i   -> node.put(name, i);
                case Long l      -> node.put(name, l);
                case Double d    -> node.put(name, d);
                case Float f     -> node.put(name, f);
                case Boolean b   -> node.put(name, b);
                case Short s     -> node.put(name, s);
                case byte[] bytes -> node.put(name, bytes);
                default          -> node.put(name, val.toString());
            }
        }

        return getObjectMapper().writeValueAsString(node);
    }

    @Override
    public TypeInformation<CdcEvent> getProducedType() {
        return TypeInformation.of(CdcEvent.class);
    }
}
