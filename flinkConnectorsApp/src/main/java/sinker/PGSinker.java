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
 * Generic Postgres sink that mirrors CDC events from any table.
 *
 * Dynamically builds upsert/delete SQL from the JSON payload in CdcEvent,
 * so no per-table POJO or SQL is needed. Only requires a primary key config
 * to know which columns form the ON CONFLICT clause.
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
 *     .sinkTo(new PGSinker(jdbcUrl, user, password, primaryKeys));
 * </pre>
 */
public class PGSinker implements Sink<CdcEvent> {
    private static final long serialVersionUID = 1L;

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final Map<String, List<String>> primaryKeys;

    public PGSinker(
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
        return new PostgresWriter(jdbcUrl, username, password, primaryKeys);
    }
}

/**
 * How Flink SinkWriter lifecycle works:
 *
 *   1. write() is called for each incoming record — we BUFFER events here, no DB call yet.
 *
 *   2. flush(endOfInput=false) is called by Flink at every checkpoint barrier.
 *      This is where we execute ALL buffered events in a single DB transaction.
 *      After flush returns, Flink knows the data is durably written and completes the checkpoint.
 *
 *   3. flush(endOfInput=true) is called once when the input stream ends (bounded source or job stop).
 *      Same logic — flush remaining buffer.
 *
 *   4. If the job crashes BEFORE flush, Flink restores from the last successful checkpoint
 *      and replays events. Since our writes are idempotent (upsert + delete), replaying is safe.
 *
 * This gives us:
 *   - Throughput: N events per transaction commit instead of N separate commits
 *   - Consistency: all events in a checkpoint interval succeed or fail together
 *   - Latency trade-off: Postgres lags behind MySQL by up to the checkpoint interval (3s)
 */
class PostgresWriter implements SinkWriter<CdcEvent> {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(PostgresWriter.class);

    private transient HikariDataSource dataSource;
    private transient ObjectMapper objectMapper;
    private final Map<String, List<String>> primaryKeys;
    // Cache of Postgres column types per table: table -> (column_name -> data_type)
    private final Map<String, Map<String, String>> columnTypeCache = new HashMap<>();
    // Buffer: events accumulate here between checkpoints
    private final List<CdcEvent> buffer = new ArrayList<>();

    PostgresWriter(
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

    /**
     * Called for each CDC event. Does NOT write to Postgres — just buffers.
     * Validates that we have PK config for the table upfront so errors surface early.
     */
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
     * e.g. "shipments" -> "\"shipments\""
     */
    private String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    /**
     * Called by Flink at each checkpoint barrier (endOfInput=false)
     * and once when the stream ends (endOfInput=true).
     *
     * Executes ALL buffered events in a single JDBC transaction:
     *   BEGIN
     *     UPSERT event 1
     *     UPSERT event 2
     *     DELETE event 3
     *     ...
     *   COMMIT
     *
     * If any event fails, the entire transaction rolls back — no partial writes.
     * Flink will then restore from the last checkpoint and replay.
     */
    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (buffer.isEmpty()) {
            return;
        }

        LOG.info("Flushing {} buffered CDC events to Postgres", buffer.size());

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false); // BEGIN transaction

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
                        executeUpsert(conn, quotedTable, pks, event.getAfter());
                    }
                }

                conn.commit(); // all events succeed together
                buffer.clear();
            } catch (Exception e) {
                conn.rollback(); // all-or-nothing: undo partial writes
                throw e;
            }
        } catch (SQLException e) {
            throw new IOException("Failed to flush CDC batch to Postgres", e);
        } catch (Exception e) {
            throw new IOException("Unexpected error flushing CDC batch", e);
        }
    }

    /**
     * Builds and executes on the shared connection (within the batch transaction):
     *   INSERT INTO {table} (col1, col2, ...) VALUES (?, ?, ...)
     *   ON CONFLICT (pk1, pk2) DO UPDATE SET col1=EXCLUDED.col1, col2=EXCLUDED.col2, ...
     */
    private void executeUpsert(Connection conn, String table, List<String> pks, String json) throws Exception {
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
        sql.append("INSERT INTO ").append(table).append(" (");
        sql.append(String.join(", ", quotedColumns));
        sql.append(") VALUES (");
        sql.append(String.join(", ", columns.stream().map(c -> "?").toList()));
        sql.append(")");

        sql.append(" ON CONFLICT (");
        sql.append(String.join(", ", quotedPks));
        sql.append(") DO UPDATE SET ");

        List<String> updateClauses = columns.stream()
                .filter(c -> !pks.contains(c))
                .map(c -> quoteIdentifier(c) + " = EXCLUDED." + quoteIdentifier(c))
                .toList();
        sql.append(String.join(", ", updateClauses));

        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < columns.size(); i++) {
                String pgType = colTypes.get(columns.get(i));
                ps.setObject(i + 1, convertValue(values.get(i), pgType));
            }
            ps.executeUpdate();
        }
    }

    /**
     * Builds and executes on the shared connection (within the batch transaction):
     *   DELETE FROM {table} WHERE pk1 = ? AND pk2 = ?
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
     * Converts a value to match the Postgres column type.
     * Handles MySQL CDC quirks like boolean sent as 0/1 integer.
     */
    private Object convertValue(Object val, String pgType) {
        if (val == null || pgType == null) {
            return val;
        }
        if ("boolean".equalsIgnoreCase(pgType) || "bool".equalsIgnoreCase(pgType)) {
            if (val instanceof Number n) {
                return n.intValue() != 0;
            }
        }
        return val;
    }

    /**
     * Queries Postgres information_schema for column types of a table.
     * Results are cached per table for the lifetime of this writer.
     */
    private Map<String, String> getColumnTypes(String table) throws SQLException {
        // table is already quoted like "shipments", strip quotes for the query
        String rawTable = table.replace("\"", "");
        if (columnTypeCache.containsKey(rawTable)) {
            return columnTypeCache.get(rawTable);
        }

        Map<String, String> types = new HashMap<>();
        String sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, rawTable);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    types.put(rs.getString("column_name"), rs.getString("data_type"));
                }
            }
        }
        columnTypeCache.put(rawTable, types);
        return types;
    }

    @Override
    public void close() throws Exception {
        // Flush any remaining events before shutting down
        flush(true);
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
