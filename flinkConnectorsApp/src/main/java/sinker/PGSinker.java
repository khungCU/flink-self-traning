package sinker;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import model.MessageNormalized;

/**
 * Generic Postgres sink that mirrors MessageNormalized events from any table.
 *
 * Uses reflection on the POJO to extract field names (camelCase → snake_case)
 * and values for dynamic SQL generation. Metadata fields (op, table) are excluded.
 *
 * Usage:
 * <pre>
 * Map<String, List<String>> primaryKeys = Map.of(
 *     "shipments", List.of("shipment_id"),
 *     "orders",    List.of("order_id")
 * );
 *
 * normalizedStream.sinkTo(new PGSinker(jdbcUrl, user, password, primaryKeys));
 * </pre>
 */
public class PGSinker implements Sink<MessageNormalized> {
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
        this.primaryKeys = primaryKeys;
    }

    @Override
    public SinkWriter<MessageNormalized> createWriter(InitContext context) throws IOException {
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
class PostgresWriter implements SinkWriter<MessageNormalized> {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(PostgresWriter.class);

    // Fields on MessageNormalized that are metadata, not database columns
    private static final Set<String> METADATA_FIELDS = Set.of("op", "table");

    private transient HikariDataSource dataSource;
    private final Map<String, List<String>> primaryKeys;
    // Buffer: events accumulate here between checkpoints
    private final List<MessageNormalized> buffer = new ArrayList<>();

    PostgresWriter(
            String jdbcUrl,
            String username,
            String password,
            Map<String, List<String>> primaryKeys
    ) {
        this.primaryKeys = primaryKeys;

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(10);
        this.dataSource = new HikariDataSource(config);
    }

    /**
     * Called for each MessageNormalized event. Does NOT write to Postgres — just buffers.
     * Validates that we have PK config for the table upfront so errors surface early.
     */
    @Override
    public void write(MessageNormalized event, Context context) throws IOException, InterruptedException {
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
                for (MessageNormalized event : buffer) {
                    String table = event.getTable();
                    List<String> pks = primaryKeys.get(table);
                    String quotedTable = quoteIdentifier(table);
                    Map<String, Object> columns = extractColumns(event);

                    if ("d".equals(event.getOp())) {
                        executeDelete(conn, quotedTable, pks, columns);
                    } else {
                        executeUpsert(conn, quotedTable, pks, columns);
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
     * Extracts database columns and values from a MessageNormalized POJO via reflection.
     * Skips static fields and metadata fields (op, table).
     * Converts camelCase field names to snake_case for Postgres.
     *
     * @return ordered map of snake_case column name -> value
     */
    private Map<String, Object> extractColumns(MessageNormalized event) throws IllegalAccessException {
        Map<String, Object> columns = new LinkedHashMap<>();
        for (Field field : event.getClass().getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            if (METADATA_FIELDS.contains(field.getName())) {
                continue;
            }
            field.setAccessible(true);
            String snakeName = camelToSnake(field.getName());
            columns.put(snakeName, field.get(event));
        }
        return columns;
    }

    /**
     * Builds and executes on the shared connection (within the batch transaction):
     *   INSERT INTO {table} (col1, col2, ...) VALUES (?, ?, ...)
     *   ON CONFLICT (pk1, pk2) DO UPDATE SET col1=EXCLUDED.col1, col2=EXCLUDED.col2, ...
     */
    private void executeUpsert(Connection conn, String table, List<String> pks, Map<String, Object> columns) throws Exception {
        List<String> colNames = new ArrayList<>(columns.keySet());
        List<Object> values = new ArrayList<>(columns.values());
        List<String> quotedColumns = colNames.stream().map(this::quoteIdentifier).toList();
        List<String> quotedPks = pks.stream().map(this::quoteIdentifier).toList();

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(table).append(" (");
        sql.append(String.join(", ", quotedColumns));
        sql.append(") VALUES (");
        sql.append(String.join(", ", colNames.stream().map(c -> "?").toList()));
        sql.append(")");

        sql.append(" ON CONFLICT (");
        sql.append(String.join(", ", quotedPks));
        sql.append(") DO UPDATE SET ");

        List<String> updateClauses = colNames.stream()
                .filter(c -> !pks.contains(c))
                .map(c -> quoteIdentifier(c) + " = EXCLUDED." + quoteIdentifier(c))
                .toList();
        sql.append(String.join(", ", updateClauses));

        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < values.size(); i++) {
                ps.setObject(i + 1, values.get(i));
            }
            ps.executeUpdate();
        }
    }

    /**
     * Builds and executes on the shared connection (within the batch transaction):
     *   DELETE FROM {table} WHERE pk1 = ? AND pk2 = ?
     */
    private void executeDelete(Connection conn, String table, List<String> pks, Map<String, Object> columns) throws Exception {
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(table).append(" WHERE ");

        List<String> whereClauses = pks.stream()
                .map(pk -> quoteIdentifier(pk) + " = ?")
                .toList();
        sql.append(String.join(" AND ", whereClauses));

        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < pks.size(); i++) {
                String pk = pks.get(i);
                ps.setObject(i + 1, columns.get(pk));
            }
            ps.executeUpdate();
        }
    }

    private static String camelToSnake(String camel) {
        return camel.replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase();
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
