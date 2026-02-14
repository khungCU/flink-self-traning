package sinker;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import model.CdcEvent;

/**
 * Generic Snowflake sink that mirrors CDC events from any table.
 *
 * Uses MERGE INTO for upserts (Snowflake does not support INSERT ... ON CONFLICT).
 * Dynamically builds MERGE/DELETE SQL from the JSON payload in CdcEvent,
 * so no per-table POJO or SQL is needed.
 *
 * JDBC URL format:
 *   jdbc:snowflake://<account>.snowflakecomputing.com/?db=<database>&schema=<schema>&warehouse=<warehouse>
 *
 * Usage:
 * <pre>
 * Map<String, List<String>> primaryKeys = Map.of(
 *     "shipments", List.of("shipment_id"),
 *     "orders",    List.of("order_id")
 * );
 *
 * cdcStream
 *     .keyBy(CdcEvent::getTable)
 *     .sinkTo(new SFSinker(jdbcUrl, user, password, primaryKeys));
 * </pre>
 *
 * SQL generated for upsert (MERGE):
 * <pre>
 * MERGE INTO "shipments" AS target
 * USING (SELECT ? AS "shipment_id", ? AS "origin", ? AS "is_arrived") AS source
 * ON target."shipment_id" = source."shipment_id"
 * WHEN MATCHED THEN UPDATE SET
 *     target."origin" = source."origin",
 *     target."is_arrived" = source."is_arrived"
 * WHEN NOT MATCHED THEN INSERT ("shipment_id", "origin", "is_arrived")
 *     VALUES (source."shipment_id", source."origin", source."is_arrived")
 * </pre>
 *
 * Comparison with PGSinker:
 * <pre>
 * ┌────────────────────┬──────────────────────────────────┬───────────────────────────────┐
 * │                    │ PGSinker (Postgres)              │ SFSinker (Snowflake)          │
 * ├────────────────────┼──────────────────────────────────┼───────────────────────────────┤
 * │ Upsert syntax      │ INSERT ... ON CONFLICT DO UPDATE │ MERGE INTO ... USING          │
 * │ Delete syntax      │ DELETE FROM ... WHERE pk = ?     │ DELETE FROM ... WHERE pk = ?   │
 * │ Boolean handling   │ Query information_schema         │ Query information_schema       │
 * │ Identifier quoting │ Double quotes (SQL standard)     │ Double quotes (SQL standard)   │
 * │ Transactions       │ setAutoCommit(false) + commit    │ setAutoCommit(false) + commit  │
 * └────────────────────┴──────────────────────────────────┴───────────────────────────────┘
 * </pre>
 */
public class SFSinker implements Sink<CdcEvent> {
    private static final long serialVersionUID = 1L;

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final Map<String, List<String>> primaryKeys;

    public SFSinker(
            String jdbcUrl,
            String username,
            String password,
            Map<String, List<String>> primaryKeys
    ) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.primaryKeys = (Serializable & Map<String, List<String>>) primaryKeys;
    }

    @Override
    public SinkWriter<CdcEvent> createWriter(InitContext context) throws IOException {
        return new SnowflakeWriter(jdbcUrl, username, password, primaryKeys);
    }
}

/**
 * SinkWriter lifecycle (same as PGSinker — see PGSinker.java for detailed docs):
 *
 *   write()  → buffer events (no DB call)
 *   flush()  → execute all buffered events in one transaction (MERGE + DELETE)
 *   close()  → flush remaining + close connection pool
 */
class SnowflakeWriter implements SinkWriter<CdcEvent> {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(SnowflakeWriter.class);

    private transient HikariDataSource dataSource;
    private transient ObjectMapper objectMapper;
    private final Map<String, List<String>> primaryKeys;
    // Cache of Snowflake column types per table: table -> (column_name -> data_type)
    private final Map<String, Map<String, String>> columnTypeCache = new HashMap<>();
    // Buffer: events accumulate here between checkpoints
    private final List<CdcEvent> buffer = new ArrayList<>();

    SnowflakeWriter(
            String jdbcUrl,
            String username,
            String password,
            Map<String, List<String>> primaryKeys
    ) {
        this.primaryKeys = primaryKeys;
        this.objectMapper = new ObjectMapper();

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(10);
        this.dataSource = new HikariDataSource(config);
    }

    @Override
    public void write(CdcEvent event, Context context) throws IOException, InterruptedException {
        String table = event.getTable();
        if (primaryKeys.get(table) == null) {
            throw new IOException("No primary key configured for table: " + table);
        }
        buffer.add(event);
    }

    /**
     * Quotes a SQL identifier to prevent SQL injection.
     * Snowflake uses double-quote quoting (same as Postgres, SQL standard).
     */
    private String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (buffer.isEmpty()) {
            return;
        }

        LOG.info("Flushing {} buffered CDC events to Snowflake", buffer.size());

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try {
                for (CdcEvent event : buffer) {
                    String table = event.getTable();
                    List<String> pks = primaryKeys.get(table);
                    String quotedTable = quoteIdentifier(table);

                    if (event.isDelete()) {
                        if (event.getBefore() == null) {
                            continue;
                        }
                        executeDelete(conn, quotedTable, pks, event.getBefore());
                    } else {
                        executeMerge(conn, quotedTable, pks, event.getAfter());
                    }
                }

                conn.commit();
                buffer.clear();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new IOException("Failed to flush CDC batch to Snowflake", e);
        } catch (Exception e) {
            throw new IOException("Unexpected error flushing CDC batch", e);
        }
    }

    /**
     * Builds and executes a MERGE statement (Snowflake's upsert):
     *
     *   MERGE INTO "shipments" AS target
     *   USING (SELECT ? AS "shipment_id", ? AS "origin", ? AS "is_arrived") AS source
     *   ON target."shipment_id" = source."shipment_id"
     *   WHEN MATCHED THEN UPDATE SET
     *       target."origin" = source."origin",
     *       target."is_arrived" = source."is_arrived"
     *   WHEN NOT MATCHED THEN INSERT ("shipment_id", "origin", "is_arrived")
     *       VALUES (source."shipment_id", source."origin", source."is_arrived")
     *
     * How MERGE works:
     *   1. The USING clause creates a virtual one-row "source" table from our values
     *   2. The ON clause joins target and source by primary key
     *   3. If a matching row exists → WHEN MATCHED → UPDATE the non-PK columns
     *   4. If no matching row    → WHEN NOT MATCHED → INSERT all columns
     *
     * This is equivalent to Postgres's INSERT ... ON CONFLICT DO UPDATE.
     */
    private void executeMerge(Connection conn, String table, List<String> pks, String json) throws Exception {
        JsonNode row = objectMapper.readTree(json);

        List<String> columns = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        row.fields().forEachRemaining(entry -> {
            columns.add(entry.getKey());
            values.add(extractValue(entry.getValue()));
        });

        Map<String, String> colTypes = getColumnTypes(table);
        List<String> quotedColumns = columns.stream().map(this::quoteIdentifier).toList();
        List<String> quotedPks = pks.stream().map(this::quoteIdentifier).toList();

        StringBuilder sql = new StringBuilder();

        // MERGE INTO "shipments" AS target
        sql.append("MERGE INTO ").append(table).append(" AS target ");

        // USING (SELECT ? AS "shipment_id", ? AS "origin", ? AS "is_arrived") AS source
        sql.append("USING (SELECT ");
        List<String> selectClauses = new ArrayList<>();
        for (String qc : quotedColumns) {
            selectClauses.add("? AS " + qc);
        }
        sql.append(String.join(", ", selectClauses));
        sql.append(") AS source ");

        // ON target."shipment_id" = source."shipment_id"
        sql.append("ON ");
        List<String> onClauses = quotedPks.stream()
                .map(pk -> "target." + pk + " = source." + pk)
                .toList();
        sql.append(String.join(" AND ", onClauses));

        // WHEN MATCHED THEN UPDATE SET target."origin" = source."origin", ...
        List<String> nonPkColumns = columns.stream()
                .filter(c -> !pks.contains(c))
                .toList();

        if (!nonPkColumns.isEmpty()) {
            sql.append(" WHEN MATCHED THEN UPDATE SET ");
            List<String> updateClauses = nonPkColumns.stream()
                    .map(c -> "target." + quoteIdentifier(c) + " = source." + quoteIdentifier(c))
                    .toList();
            sql.append(String.join(", ", updateClauses));
        }

        // WHEN NOT MATCHED THEN INSERT ("shipment_id", "origin", ...) VALUES (source."shipment_id", ...)
        sql.append(" WHEN NOT MATCHED THEN INSERT (");
        sql.append(String.join(", ", quotedColumns));
        sql.append(") VALUES (");
        List<String> sourceRefs = quotedColumns.stream()
                .map(qc -> "source." + qc)
                .toList();
        sql.append(String.join(", ", sourceRefs));
        sql.append(")");

        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < columns.size(); i++) {
                String sfType = colTypes.get(columns.get(i));
                ps.setObject(i + 1, convertValue(values.get(i), sfType));
            }
            ps.executeUpdate();
        }
    }

    /**
     * Builds and executes:
     *   DELETE FROM "shipments" WHERE "shipment_id" = ?
     *
     * Same as PGSinker — DELETE syntax is identical in Snowflake.
     */
    private void executeDelete(Connection conn, String table, List<String> pks, String json) throws Exception {
        JsonNode row = objectMapper.readTree(json);
        Map<String, String> colTypes = getColumnTypes(table);

        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(table).append(" WHERE ");

        List<String> whereClauses = pks.stream()
                .map(pk -> quoteIdentifier(pk) + " = ?")
                .toList();
        sql.append(String.join(" AND ", whereClauses));

        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < pks.size(); i++) {
                String pk = pks.get(i);
                Object val = extractValue(row.get(pk));
                ps.setObject(i + 1, convertValue(val, colTypes.get(pk)));
            }
            ps.executeUpdate();
        }
    }

    private Object extractValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        return switch (node.getNodeType()) {
            case NUMBER -> node.isInt() ? node.intValue()
                         : node.isLong() ? node.longValue()
                         : node.isFloat() ? node.floatValue()
                         : node.doubleValue();
            case BOOLEAN -> node.booleanValue();
            case STRING  -> node.textValue();
            default      -> node.asText();
        };
    }

    /**
     * Converts a value to match the Snowflake column type.
     * Handles MySQL CDC quirks like boolean sent as 0/1 integer.
     *
     * Snowflake reports BOOLEAN as "BOOLEAN" in information_schema
     * (unlike Postgres which uses "boolean" or "bool").
     */
    private Object convertValue(Object val, String sfType) {
        if (val == null || sfType == null) {
            return val;
        }
        if ("BOOLEAN".equalsIgnoreCase(sfType)) {
            if (val instanceof Number n) {
                return n.intValue() != 0;
            }
        }
        return val;
    }

    /**
     * Queries Snowflake information_schema for column types of a table.
     * Results are cached per table for the lifetime of this writer.
     *
     * Snowflake's information_schema uses uppercase column names by default,
     * but the query is case-insensitive. We lowercase the column names in the
     * cache to match the CDC event JSON keys (which come from MySQL in lowercase).
     */
    private Map<String, String> getColumnTypes(String table) throws SQLException {
        String rawTable = table.replace("\"", "");
        if (columnTypeCache.containsKey(rawTable)) {
            return columnTypeCache.get(rawTable);
        }

        Map<String, String> types = new HashMap<>();
        String sql = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, rawTable.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    // Lowercase the column name to match MySQL CDC JSON keys
                    String colName = rs.getString("COLUMN_NAME").toLowerCase();
                    String dataType = rs.getString("DATA_TYPE");
                    types.put(colName, dataType);
                }
            }
        }
        columnTypeCache.put(rawTable, types);
        return types;
    }

    @Override
    public void close() throws Exception {
        flush(true);
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
