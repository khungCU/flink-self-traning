package reconciliation;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.util.OutputTag;

import model.MessageNormalized;
import model.Shipment;
import model.ShipmentV0;

/**
 * Single source of truth for all per-table CDC configuration.
 *
 * Centralizes three concerns that were previously scattered across Main.java
 * and SchemaNormalizer:
 *   1. POJO class — which Java class to deserialize each table's JSON into
 *   2. Primary keys — which columns form the PK (used for upsert/delete in PGSinker)
 *   3. OutputTag — Flink side-output tag for routing events per table
 *
 * Adding a new table = one register() call in the constructor. No other files
 * need editing — SchemaNormalizer, PGSinker, and Main all derive their config
 * from this registry.
 */
public class TableRegistry implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Immutable config bundle for a single table.
     *
     * @param pojoClass  the MessageNormalized subclass that Jackson deserializes into
     * @param primaryKeys  column names (snake_case) used in ON CONFLICT / WHERE clauses
     * @param outputTag  Flink side-output tag for routing this table's normalized events
     */
    public record TableConfig(
            Class<? extends MessageNormalized> pojoClass,
            List<String> primaryKeys,
            OutputTag<MessageNormalized> outputTag
    ) implements Serializable {}

    private final Map<String, TableConfig> tables = new HashMap<>();

    /** Side-output tag for tables not registered here — used for schema-drift alerting. */
    private final OutputTag<MessageNormalized> unknownTag = new OutputTag<MessageNormalized>("schema-drift"){};

    /**
     * Registers all known CDC tables.
     * To add a new table, add one register() call here.
     */
    public TableRegistry() {
        register("shipments",    Shipment.class,   List.of("shipment_id"));
        register("shipments_v0", ShipmentV0.class,  List.of("shipment_id"));
    }

    /**
     * Registers a table with its POJO class and primary key columns.
     * Automatically creates an OutputTag named after the table.
     *
     * Note: OutputTag requires an anonymous subclass ({@code new OutputTag<T>(id){}})
     * so Flink can capture the generic type at runtime via TypeHint.
     */
    private void register(String table, Class<? extends MessageNormalized> pojoClass, List<String> primaryKeys) {
        OutputTag<MessageNormalized> tag = new OutputTag<MessageNormalized>(table){};
        tables.put(table, new TableConfig(pojoClass, primaryKeys, tag));
    }

    /**
     * Returns table name → OutputTag mapping.
     * Used by SchemaNormalizer to route each CDC event to the correct side output.
     */
    public Map<String, OutputTag<MessageNormalized>> getTableTags() {
        Map<String, OutputTag<MessageNormalized>> result = new HashMap<>();
        tables.forEach((name, config) -> result.put(name, config.outputTag()));
        return result;
    }

    /**
     * Returns table name → primary key columns mapping.
     * Used by PGSinker to build ON CONFLICT (upsert) and DELETE WHERE clauses.
     *
     * Returns a HashMap which implements Serializable — safe to store as a
     * field in Flink Sink implementations without intersection casts.
     */
    public Map<String, List<String>> getPrimaryKeys() {
        Map<String, List<String>> result = new HashMap<>();
        tables.forEach((name, config) -> result.put(name, config.primaryKeys()));
        return result;
    }

    /**
     * Returns table name → POJO class mapping.
     * Used by SchemaNormalizer to deserialize CDC JSON into the correct typed POJO
     * via Jackson ObjectMapper.
     */
    public Map<String, Class<? extends MessageNormalized>> getTableClasses() {
        Map<String, Class<? extends MessageNormalized>> result = new HashMap<>();
        tables.forEach((name, config) -> result.put(name, config.pojoClass()));
        return result;
    }

    /**
     * Returns the side-output tag for unrecognized tables.
     * Events from tables not registered here are emitted to this tag
     * so downstream operators can log or alert on schema drift.
     */
    public OutputTag<MessageNormalized> getUnknownTag() {
        return unknownTag;
    }

    /**
     * Returns all known-table OutputTags.
     * Used by Main to union all per-table side outputs into a single stream
     * before sinking to Postgres.
     */
    public Collection<OutputTag<MessageNormalized>> getAllKnownTags() {
        return tables.values().stream().map(TableConfig::outputTag).toList();
    }

    /**
     * Checks whether a table name has been registered.
     */
    public boolean isKnownTable(String table) {
        return tables.containsKey(table);
    }
}
