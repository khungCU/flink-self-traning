package deserializer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import model.Shipment;
import model.ShipmentCdcEvent;

/**
 * Custom Debezium deserializer that converts CDC records directly into ShipmentCdcEvent POJOs.
 */
public class ShipmentDebeziumDeserializer implements DebeziumDeserializationSchema<ShipmentCdcEvent> {
    private static final long serialVersionUID = 1L;

    @Override
    public void deserialize(SourceRecord record, Collector<ShipmentCdcEvent> out) throws Exception {
        Struct value = (Struct) record.value();
        if (value == null) {
            return;
        }

        ShipmentCdcEvent event = new ShipmentCdcEvent();

        // Extract operation type
        String op = value.getString("op");
        event.setOp(op);

        // Extract timestamp
        Long tsMs = value.getInt64("ts_ms");
        if (tsMs != null) {
            event.setTsMs(tsMs);
        }

        // Extract source metadata
        Struct source = value.getStruct("source");
        if (source != null) {
            event.setDatabase(source.getString("db"));
            event.setTable(source.getString("table"));
        }

        // Parse before struct (present for updates and deletes)
        Struct before = value.getStruct("before");
        if (before != null) {
            event.setBefore(structToShipment(before));
        }

        // Parse after struct (present for inserts and updates)
        Struct after = value.getStruct("after");
        if (after != null) {
            event.setAfter(structToShipment(after));
        }

        out.collect(event);
    }

    /**
     * Converts a Kafka Connect Struct to a Shipment POJO.
     */
    private Shipment structToShipment(Struct struct) {
        Shipment shipment = new Shipment();

        // Map struct fields to POJO
        // Field names must match the MySQL column names
        shipment.setShipmentId(struct.getInt32("shipment_id"));
        shipment.setOrderId(struct.getInt32("order_id"));
        shipment.setOrigin(struct.getString("origin"));
        shipment.setDestination(struct.getString("destination"));

        // Boolean fields in MySQL CDC come as Integer (0/1) or Boolean
        Object isArrivedValue = struct.get("is_arrived");
        if (isArrivedValue != null) {
            if (isArrivedValue instanceof Boolean) {
                shipment.setIsArrived((Boolean) isArrivedValue);
            } else if (isArrivedValue instanceof Integer) {
                shipment.setIsArrived(((Integer) isArrivedValue) != 0);
            } else if (isArrivedValue instanceof Short) {
                shipment.setIsArrived(((Short) isArrivedValue) != 0);
            }
        }

        return shipment;
    }

    /*
    * getProducedType() tells Flink the type information of the output records from your deserializer.
     */
    @Override
    public TypeInformation<ShipmentCdcEvent> getProducedType() {
        return TypeInformation.of(ShipmentCdcEvent.class);
    }
}
