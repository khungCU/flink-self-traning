# Flink CDC MySQL Source Connector Demo

This project demonstrates how to use the Flink CDC (Change Data Capture) connector to capture real-time changes from a MySQL database.

## Prerequisites

- Java 21
- Gradle
- Docker & Docker Compose

## Project Structure

```
flinkConnectorsApp/
├── src/main/java/Main.java          # Main Flink application
├── src/main/resources/
│   └── application.properties       # MySQL connection configuration
└── build.gradle                     # Dependencies (Flink 1.18.0, MySQL CDC 3.2.1)
```

## Quick Start

### 1. Start Infrastructure

```bash
docker compose up -d
```

This starts:
- MySQL (port 3306) with initial schema from `mysql-init/db1.sql`
- Kafka broker (port 9092)
- Flink JobManager & TaskManager

### 2. Grant MySQL Replication Permissions

The CDC connector requires MySQL replication privileges. Run the following command:

```bash
docker exec flink-self-traning-mysql-1 mysql -uroot -p123456 -e "
  GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'mysqluser'@'%';
  GRANT SELECT ON db_1.* TO 'mysqluser'@'%';
  FLUSH PRIVILEGES;
  SHOW GRANTS FOR 'mysqluser'@'%';
"
```

### 3. Configure application.properties

Create `flinkConnectorsApp/src/main/resources/application.properties`:

```properties
mysql.hostname=localhost
mysql.port=3306
mysql.database=db_1
mysql.table=db_1.shipments
mysql.username=mysqluser
mysql.password=mysqlpw
```

### 4. Run the Application

```bash
cd flinkConnectorsApp
../gradlew run
```

## Essential CDC Configuration

### MySQL CDC Source Builder Parameters

| Parameter | Description | Example Value |
|-----------|-------------|---------------|
| `hostname` | MySQL server host | `localhost` |
| `port` | MySQL server port | `3306` |
| `databaseList` | Database(s) to monitor | `db_1` |
| `tableList` | Table(s) to capture (format: `db.table`) | `db_1.shipments` |
| `username` | MySQL user with replication privileges | `mysqluser` |
| `password` | MySQL user password | `mysqlpw` |
| `serverId` | Unique server ID range for this connector | `5400-5404` |
| `serverTimeZone` | MySQL server timezone | `UTC` |

### Required MySQL Permissions

The MySQL user must have these privileges:
- `REPLICATION SLAVE` - Read binary log events
- `REPLICATION CLIENT` - Connect as a replication client
- `SELECT` - Read table data for initial snapshot

### Checkpointing

Checkpointing is required for CDC sources to track binlog position:

```java
env.enableCheckpointing(3000); // checkpoint every 3 seconds
```

### Parallelism

MySQL binlog is a single stream, so the source parallelism must be set to 1:

```java
env.fromSource(mySQLSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
   .setParallelism(1);
```

## Understanding MySQL Binlog and Flink CDC

### How MySQL Binlog Works

MySQL uses binary logs (binlog) to record all changes to the database. The Flink CDC connector acts as a **MySQL replication slave** to read these binlog events in real-time.

```
┌─────────────────┐     binlog events     ┌─────────────────┐
│  MySQL Master   │ ───────────────────►  │  Flink CDC      │
│  (your DB)      │   replication         │  (acts as slave)│
└─────────────────┘                       └─────────────────┘
```

**Key concept:** Each replication slave must have a **unique server-id** to identify itself to the MySQL master. This is why the `serverId` parameter is critical.

### Server ID Configuration

The `serverId` parameter must be globally unique across all connections reading from the same MySQL server:

```java
.serverId("5400-5404")  // Range allows parallelism up to 5
```

**Why a range?** When using parallel snapshot reading, each parallel reader needs its own server-id. The range `5400-5404` allows up to 5 parallel readers.

**Conflicts occur when:**
1. Another Flink CDC job is using the same server-id range
2. A previous job didn't shut down cleanly and the connection is still active
3. Other CDC tools (Debezium, Canal, Maxwell) are using the same server-id
4. MySQL replication slaves are configured with overlapping IDs

### Startup Options

The connector supports different startup modes via `StartupOptions`:

```java
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;

MySqlSource.<T>builder()
    // ... other configurations
    .startupOptions(StartupOptions.initial())  // or other options
    .build();
```

| Startup Option | Description | Use Case |
|----------------|-------------|----------|
| `StartupOptions.initial()` | **(Default)** Takes a full snapshot of existing data, then switches to binlog streaming | First-time setup, need complete data |
| `StartupOptions.latest()` | Skips snapshot, starts from the latest binlog position | Only care about new changes, existing data already processed |
| `StartupOptions.earliest()` | Skips snapshot, starts from the earliest available binlog | Replay all available history (binlog retention dependent) |
| `StartupOptions.specificOffset(file, pos)` | Start from a specific binlog file and position | Recovery scenarios, precise control |
| `StartupOptions.timestamp(timestamp)` | Start from events after a specific timestamp | Time-based recovery |

**Example - Skip snapshot and only capture new changes:**

```java
MySqlSource<ShipmentCdcEvent> source = MySqlSource.<ShipmentCdcEvent>builder()
    .hostname("localhost")
    .port(3306)
    .databaseList("db_1")
    .tableList("db_1.shipments")
    .username("mysqluser")
    .password("mysqlpw")
    .serverId("5400-5404")
    .deserializer(new ShipmentDebeziumDeserializer())
    .startupOptions(StartupOptions.latest())  // No snapshot
    .build();
```

### Binlog Position Tracking

Flink CDC uses **checkpointing** to track the current binlog position. This ensures:
- Exactly-once processing semantics
- Recovery from failures without data loss or duplication

```java
env.enableCheckpointing(3000);  // Checkpoint every 3 seconds
```

**Important:** Without checkpointing enabled, the connector cannot track binlog positions and may fail or lose data on restart.

## Testing CDC

### Insert Data

```bash
docker exec -it flink-self-traning-mysql-1 mysql -umysqluser -pmysqlpw db_1 -e "
  INSERT INTO shipments VALUES (1, 1, 'Shanghai', 'Tokyo', false);
"
```

### Update Data

```bash
docker exec -it flink-self-traning-mysql-1 mysql -umysqluser -pmysqlpw db_1 -e "
  UPDATE shipments SET is_arrived = true WHERE shipment_id = 1;
"
```

### Delete Data

```bash
docker exec -it flink-self-traning-mysql-1 mysql -umysqluser -pmysqlpw db_1 -e "
  DELETE FROM shipments WHERE shipment_id = 1;
"
```

The application will print CDC events in Debezium JSON format showing the operation type (c=create, u=update, d=delete) and the before/after values.

## Dependencies

Key dependencies in `build.gradle`:

```groovy
ext {
    flinkVersion = '1.18.0'
}

dependencies {
    implementation "org.apache.flink:flink-streaming-java:${flinkVersion}"
    implementation 'org.apache.flink:flink-connector-mysql-cdc:3.2.1'
    implementation "org.apache.flink:flink-connector-base:${flinkVersion}"
}
```

## Troubleshooting

### Java Module Access Error

If you encounter `InaccessibleObjectException`, add these JVM args:

```
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
```

For VSCode, configure `.vscode/launch.json`:

```json
{
    "type": "java",
    "name": "Main",
    "request": "launch",
    "mainClass": "Main",
    "projectName": "flinkConnectorsApp",
    "vmArgs": "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"
}
```

### Permission Denied on MySQL

Ensure the MySQL user has replication privileges (see Step 2 above).

### Connection Refused

Make sure Docker containers are running:

```bash
docker compose ps
```

### Debezium NoSuchMethodError

**Error message:**
```
java.lang.NoSuchMethodError: 'void io.debezium.connector.mysql.MySqlConnection$MySqlConnectionConfiguration.<init>(io.debezium.config.Configuration, java.util.Properties)'
```

**Cause:** Version mismatch between `flink-connector-mysql-cdc` and the bundled Debezium library. This typically occurs with CDC connector version 3.1.1.

**Solution:** Upgrade to a compatible CDC connector version:

```groovy
// In build.gradle, use 3.2.1 or later
implementation 'org.apache.flink:flink-connector-mysql-cdc:3.2.1'
```

After updating, clean and rebuild:
```bash
./gradlew clean build --refresh-dependencies
```

**IDE cache issue:** If running from an IDE (VS Code, IntelliJ), the IDE may cache the old dependency. Either:
1. Refresh Gradle projects in your IDE
2. Run via Gradle CLI: `./gradlew :flinkConnectorsApp:run`

### Server ID Conflict Error

**Error message:**
```
A slave with the same server_uuid/server_id as this slave has connected to the master
```

**Cause:** Another process is using the same server-id to connect to MySQL's binlog.

**Solutions:**

1. **Change the server-id to a unique value:**
   ```java
   .serverId("6100-6104")  // Use a different range
   ```

2. **Check for lingering MySQL connections and kill them:**
   ```bash
   # Show all MySQL connections - look for "Binlog Dump" in the Command column
   docker exec flink-self-traning-mysql-1 mysql -uroot -p123456 -e "SHOW PROCESSLIST;"
   ```

   Example output:
   ```
   Id   User       Host              db     Command      Time  State
   93   mysqluser  192.168.65.1:xxx  NULL   Binlog Dump  267   Master has sent all binlog to slave
   133  mysqluser  192.168.65.1:xxx  NULL   Binlog Dump  32    Master has sent all binlog to slave
   ```

   Kill the `Binlog Dump` connections using their IDs:
   ```bash
   docker exec flink-self-traning-mysql-1 mysql -uroot -p123456 -e "KILL 93; KILL 133;"
   ```

   Verify no binlog connections remain:
   ```bash
   docker exec flink-self-traning-mysql-1 mysql -uroot -p123456 -e "SHOW PROCESSLIST;" | grep "Binlog"
   ```

3. **Stop other CDC tools** (Debezium, Canal) that might be connected to the same MySQL instance.

4. **Use a random server-id** to avoid conflicts in development:
   ```java
   .serverId(String.valueOf(new java.util.Random().nextInt(10000) + 10000))
   ```

**Tip:** To avoid this issue, always stop your Flink job gracefully (Ctrl+C) before restarting. If you force-kill the process, the MySQL binlog connection may linger and cause server-id conflicts on the next run.

### Binlog Not Available Error

**Error message:**
```
The connector is trying to read binlog but the binlog position is no longer available
```

**Cause:** MySQL has purged old binlog files, and the connector is trying to resume from a position that no longer exists.

**Solutions:**

1. **Use `StartupOptions.latest()`** to start from current position:
   ```java
   .startupOptions(StartupOptions.latest())
   ```

2. **Use `StartupOptions.initial()`** to take a fresh snapshot:
   ```java
   .startupOptions(StartupOptions.initial())
   ```

3. **Increase MySQL binlog retention:**
   ```sql
   SET GLOBAL binlog_expire_logs_seconds = 604800;  -- 7 days
   ```

4. **Reset MySQL binlog (Nuclear option - development only):**

   ⚠️ **Warning:** This deletes all binlog history. Only use in development environments.

   ```bash
   # Reset binlog to fresh state
   docker exec flink-self-traning-mysql-1 mysql -uroot -p123456 -e "RESET MASTER;"

   # Verify the new binlog position
   docker exec flink-self-traning-mysql-1 mysql -uroot -p123456 -e "SHOW MASTER STATUS;"
   ```

   Expected output after reset:
   ```
   File             Position  Binlog_Do_DB  Binlog_Ignore_DB  Executed_Gtid_Set
   mysql-bin.000001 154
   ```

   After resetting, your Flink CDC application will start reading from this fresh binlog position.

### Snapshot Takes Too Long

For large tables, the initial snapshot can take a long time.

**Solutions:**

1. **Skip snapshot** if you only need new changes:
   ```java
   .startupOptions(StartupOptions.latest())
   ```

2. **Enable parallel snapshot reading** (for tables with primary key):
   ```java
   .splitSize(8096)  // Split size for parallel reading
   ```

## MySQL to Postgres Table Mirroring

This project includes a generic CDC pipeline that mirrors MySQL tables to Postgres in real-time. It captures insert, update, and delete events from multiple MySQL tables and applies them to the corresponding Postgres tables using upsert (`ON CONFLICT`) and delete operations.

### How It Works

```
MySQL (source)                    Flink Pipeline                         Postgres (destination)
┌──────────────┐     binlog      ┌──────────────────────┐     upsert    ┌──────────────────┐
│  shipments   │ ──────────────► │  JsonCdcDeserializer  │ ───────────► │  shipments       │
│  shipments_v0│    CDC events   │  (any table → JSON)   │   /delete    │  shipments_v0    │
└──────────────┘                 │         │              │              └──────────────────┘
                                 │    keyBy(table)        │
                                 │         │              │
                                 │    PGSinker            │
                                 │  (dynamic SQL from JSON│
                                 └──────────────────────┘
```

**Key components:**

| Component | File | Purpose |
|-----------|------|---------|
| `JsonCdcDeserializer` | `deserializer/JsonCdcDeserializer.java` | Converts Debezium CDC records from any table into a generic `CdcEvent` with JSON before/after payloads |
| `CdcEvent` | `model/CdcEvent.java` | Generic CDC event model — stores before/after as JSON strings instead of typed POJOs, so it works with any table schema |
| `PGSinker` | `sinker/PGSinker.java` | Generic Postgres sink that dynamically builds upsert/delete SQL from the JSON payload. No per-table code needed |

**CDC operation mapping:**

| MySQL Operation | CDC `op` | Postgres Action |
|-----------------|----------|-----------------|
| INSERT | `c` | `INSERT ... ON CONFLICT DO UPDATE` |
| Snapshot read | `r` | `INSERT ... ON CONFLICT DO UPDATE` |
| UPDATE | `u` | `INSERT ... ON CONFLICT DO UPDATE` |
| DELETE | `d` | `DELETE FROM ... WHERE pk = ?` |

### Code Walkthrough & Essential Concepts

#### Project Structure (Mirror Pipeline)

```
flinkConnectorsApp/src/main/java/
├── Main.java                              # Entry point — wires source → transform → sink
├── model/
│   ├── CdcEvent.java                      # Generic CDC event (JSON-based, any table)
│   ├── ShipmentCdcEvent.java              # Single-table CDC event (typed POJO, legacy)
│   └── Shipment.java                      # Shipment POJO (used by ShipmentCdcEvent)
├── deserializer/
│   └── JsonCdcDeserializer.java           # Debezium → CdcEvent (table-agnostic)
└── sinker/
    └── PGSinker.java                      # CdcEvent → Postgres (dynamic SQL, batched)
```

#### Class Relationship Diagram

```
                        ┌──────────────────────────────────────────┐
                        │              Main.java                    │
                        │  (orchestrator — wires the pipeline)      │
                        └──────┬──────────────┬──────────────┬─────┘
                               │              │              │
                    creates     │    creates    │    creates   │
                               ▼              ▼              ▼
                  ┌─────────────────┐ ┌──────────────┐ ┌───────────┐
                  │ MySqlSource     │ │ DataStream   │ │ PGSinker  │
                  │ <CdcEvent>      │ │ <CdcEvent>   │ │(Sink API) │
                  │                 │ │              │ │           │
                  │ uses ▼          │ │ .keyBy()     │ │ creates ▼ │
                  │ JsonCdc         │ │ .sinkTo()    │ │ Postgres  │
                  │ Deserializer    │ │              │ │ Writer    │
                  └────────┬────────┘ └──────────────┘ └─────┬─────┘
                           │                                  │
                    produces│                           consumes│
                           ▼                                  ▼
                     ┌──────────┐                      ┌──────────┐
                     │ CdcEvent │ ────────────────────►│ CdcEvent │
                     │ (model)  │   flows through      │ .getAfter()  → JSON → SQL
                     └──────────┘   the DataStream     │ .getBefore() → JSON → SQL
                                                       │ .getTable()  → route to table
                                                       └──────────┘
```

**Data flow:** MySQL binlog → `SourceRecord` (Kafka Connect) → `JsonCdcDeserializer` → `CdcEvent` → `DataStream` → `keyBy(table)` → `PGSinker` → `PostgresWriter.write()` (buffer) → `flush()` (batch transaction) → Postgres

---

#### 1. `model/CdcEvent.java` — The Data Contract

**Purpose:** A single class that represents CDC events from ANY table.

**Why JSON strings instead of typed POJOs?**

```
ShipmentCdcEvent (legacy, single-table)     CdcEvent (generic, multi-table)
┌────────────────────────────┐              ┌────────────────────────────┐
│ before: Shipment (POJO)    │              │ before: String (JSON)      │
│ after:  Shipment (POJO)    │              │ after:  String (JSON)      │
│                            │              │ table:  "shipments"        │
│ Only works for shipments!  │              │ Works for ANY table!       │
└────────────────────────────┘              └────────────────────────────┘
```

If you have 50 tables, the POJO approach requires 50 model classes + 50 deserializers. The JSON approach needs just `CdcEvent` + `JsonCdcDeserializer`.

**Essential Java concept — `Serializable`:**

```java
public class CdcEvent implements Serializable {
    private static final long serialVersionUID = 1L;
```

Flink sends objects between task managers over the network. Any object flowing through a `DataStream` must implement `Serializable` so Java can convert it to bytes and back. The `serialVersionUID` ensures deserialization compatibility if the class is modified.

**Essential CDC concept — operation types:**

```java
// Debezium CDC operation codes
public boolean isInsert() { return "c".equals(op) || "r".equals(op); }  // "r" = snapshot read
public boolean isUpdate() { return "u".equals(op); }
public boolean isDelete() { return "d".equals(op); }
```

The `"r"` (read) operation happens during initial snapshot — Debezium reads existing rows and emits them as `"r"` events. They are functionally identical to inserts.

---

#### 2. `deserializer/JsonCdcDeserializer.java` — Bridging Debezium to Flink

**Purpose:** Converts raw Debezium `SourceRecord` (Kafka Connect internal format) into our `CdcEvent`.

**Why is this needed?** The MySQL CDC source connector internally uses Debezium, which produces Kafka Connect `SourceRecord` objects. Flink doesn't know how to handle these natively — you provide a `DebeziumDeserializationSchema<T>` to tell it how to convert each record into your type `T`.

**Essential Flink concept — `DebeziumDeserializationSchema`:**

```java
public class JsonCdcDeserializer implements DebeziumDeserializationSchema<CdcEvent> {

    // Called once per CDC event. You extract fields and emit via collector.
    public void deserialize(SourceRecord record, Collector<CdcEvent> out) { ... }

    // Tells Flink the output type (needed for serialization/optimization).
    public TypeInformation<CdcEvent> getProducedType() { ... }
}
```

This follows the same pattern as Kafka's `DeserializationSchema` — Flink sources need to know how to turn raw bytes/records into typed Java objects.

**Essential Kafka Connect concept — `Struct` (the Debezium data format):**

A Debezium CDC record is a nested `Struct` (not JSON, not a POJO):

```
SourceRecord.value() → Struct {
    "op":     "c"                          ← operation type (String)
    "ts_ms":  1700000000000                ← timestamp (Long)
    "source": Struct {                     ← metadata
        "db":    "db_1"
        "table": "shipments"
    }
    "before": Struct { ... } or null       ← row before change
    "after":  Struct {                     ← row after change
        "shipment_id": 1
        "order_id":    100
        "is_arrived":  0                   ← TINYINT(1), NOT boolean!
    }
}
```

The `Struct` has a schema (field names + types) attached. This is how `structToJson()` iterates over fields without knowing column names in advance:

```java
for (Field field : struct.schema().fields()) {   // schema-driven iteration
    String name = field.name();                    // column name
    Object val = struct.get(field);                // column value (typed)
}
```

**Essential Jackson concept — `ObjectMapper` and `ObjectNode` (writing JSON):**

Jackson is the standard Java library for JSON processing. In the deserializer, we use it to **build** JSON strings from Struct data:

```java
ObjectMapper objectMapper = new ObjectMapper();           // the central Jackson entry point
ObjectNode node = objectMapper.createObjectNode();        // creates a mutable JSON object: {}

node.put("shipment_id", 1);                               // { "shipment_id": 1 }
node.put("origin", "Shanghai");                           // { "shipment_id": 1, "origin": "Shanghai" }
node.putNull("destination");                              // { ..., "destination": null }

String json = objectMapper.writeValueAsString(node);      // serialize to JSON string
// → '{"shipment_id":1,"origin":"Shanghai","destination":null}'
```

Think of it as two roles:
- **`ObjectMapper`** — the engine. It serializes objects to JSON (`writeValueAsString`) and deserializes JSON back to objects (`readTree`, `readValue`). Thread-safe, reusable — create once, use everywhere.
- **`ObjectNode`** — a mutable JSON object builder. It represents a `{ }` node that you can add key-value pairs to with `.put()`. It's part of Jackson's **Tree Model** — an in-memory representation of JSON, similar to how the DOM represents HTML.

Why not just use `String.format()` or string concatenation to build JSON? Because Jackson handles escaping, null values, numeric precision, and nested structures correctly. Manual string building is fragile and error-prone.

**Essential Java 21 concept — switch pattern matching:**

```java
switch (val) {
    case null        -> node.putNull(name);     // null-safe handling
    case Integer i   -> node.put(name, i);      // auto-unbox + type bind
    case Short s     -> node.put(name, s);      // MySQL TINYINT(1) lands here
    case Boolean b   -> node.put(name, b);
    default          -> node.put(name, val.toString());
}
```

This replaces verbose `if (val instanceof Integer) { Integer i = (Integer) val; ... }` chains. The variable after the type (e.g. `Integer i`) is a **binding variable** — it's automatically cast and scoped to that branch.

**Essential Java concept — `transient` and lazy initialization:**

```java
private transient ObjectMapper objectMapper;   // not serialized

private ObjectMapper getObjectMapper() {
    if (objectMapper == null) {                 // recreate after deserialization
        objectMapper = new ObjectMapper();
    }
    return objectMapper;
}
```

Flink serializes the deserializer when distributing it to task managers. `ObjectMapper` is not serializable, so it's marked `transient` (excluded from serialization). After deserialization on the task manager, the field is `null` — the lazy getter recreates it on first use. This is a common Flink pattern for non-serializable resources.

---

#### 3. `sinker/PGSinker.java` — The Flink Sink2 API + JDBC + Batching

**Purpose:** Receives `CdcEvent` objects and writes them to Postgres. Handles any table dynamically.

This file has the most concepts packed in — Flink Sink API, JDBC transactions, connection pooling, and batching.

**Essential Flink concept — Sink2 API (two classes):**

```
┌──────────────────────────────┐        ┌──────────────────────────────────┐
│ PGSinker implements Sink     │        │ PostgresWriter implements        │
│                              │        │           SinkWriter             │
│ • Factory — creates writers  │        │                                  │
│ • Serializable (distributed  │        │ • Does the actual work           │
│   to task managers)          │        │ • write(): buffer events         │
│                              │        │ • flush(): batch execute to DB   │
│ createWriter() ─────────────────────► │ • close(): cleanup resources     │
└──────────────────────────────┘        └──────────────────────────────────┘
    (runs on JobManager,                     (runs on TaskManager,
     serialized + shipped)                    one instance per subtask)
```

Why two classes? `Sink` is a factory that gets serialized and sent to each task manager. `SinkWriter` is the actual worker that holds non-serializable resources (DB connections, object mappers). This separation is required by Flink's distributed architecture.

**Essential Flink concept — SinkWriter lifecycle (write → flush → close):**

```
                    Flink runtime calls
                    ┌─────────────────────────────────────────────┐
                    │                                             │
    CDC event 1 ──► │  write(event1)  →  buffer: [e1]            │
    CDC event 2 ──► │  write(event2)  →  buffer: [e1, e2]        │
    CDC event 3 ──► │  write(event3)  →  buffer: [e1, e2, e3]    │
                    │                                             │
    ─── checkpoint  │  flush(false)   →  BEGIN                    │
        barrier ──► │                      UPSERT e1              │
                    │                      UPSERT e2              │
                    │                      DELETE e3              │
                    │                    COMMIT                   │
                    │                    buffer.clear()           │
                    │                                             │
    ─── checkpoint completes (binlog offset saved) ──────────────│
                    │                                             │
    CDC event 4 ──► │  write(event4)  →  buffer: [e4]            │
                    │  ...                                        │
                    │                                             │
    ─── job ends ─► │  close()                                   │
                    │    └── flush(true) → flush remaining buffer │
                    │    └── dataSource.close()                   │
                    └─────────────────────────────────────────────┘
```

**Why batching matters:** Without batching, each event opens a connection, runs one SQL, and commits. The commit is the expensive part — it forces Postgres to write to disk (fsync). With batching, N events share one commit. For 100 events per checkpoint, that's 100x fewer fsyncs.

**Essential JDBC concept — transactions and autocommit:**

```java
conn.setAutoCommit(false);   // BEGIN — starts a transaction
// ... execute multiple statements ...
conn.commit();               // COMMIT — all writes become visible atomically
// or
conn.rollback();             // ROLLBACK — discard all writes since BEGIN
```

By default, JDBC runs in autocommit mode — every statement is its own transaction. Setting `autoCommit(false)` groups multiple statements into one atomic transaction. Either ALL succeed (commit) or NONE succeed (rollback). This is the foundation of the "all-or-nothing" flush behavior.

**Essential JDBC concept — `PreparedStatement` and SQL injection prevention:**

```java
// BAD: string concatenation → SQL injection
String sql = "DELETE FROM " + table + " WHERE id = " + id;

// GOOD: parameterized query → safe
PreparedStatement ps = conn.prepareStatement("DELETE FROM \"shipments\" WHERE \"id\" = ?");
ps.setObject(1, id);    // ? placeholder filled safely
```

`PreparedStatement` separates SQL structure from data values. The `?` placeholders are filled by `setObject()`, which properly escapes values. For identifiers (table/column names) which can't use `?`, we use `quoteIdentifier()` to double-quote them.

**Essential Jackson concept — `JsonNode` and `ObjectMapper.readTree()` (reading JSON):**

In the deserializer, we used `ObjectMapper` + `ObjectNode` to **write** JSON. In the sink, we do the reverse — **read** JSON back into a traversable tree using `JsonNode`:

```java
ObjectMapper objectMapper = new ObjectMapper();

// Parse JSON string → JsonNode (read-only tree)
String json = "{\"shipment_id\":1,\"origin\":\"Shanghai\",\"is_arrived\":0}";
JsonNode row = objectMapper.readTree(json);

// Navigate the tree
row.get("shipment_id")          // → JsonNode representing 1
row.get("shipment_id").intValue()  // → 1 (as Java int)
row.get("origin").textValue()      // → "Shanghai" (as Java String)
row.get("nonexistent")             // → null (no exception thrown)

// Iterate over all fields (used to build dynamic SQL)
row.fields().forEachRemaining(entry -> {
    String columnName = entry.getKey();     // "shipment_id", "origin", ...
    JsonNode value    = entry.getValue();   // JsonNode for each value
});
```

**`JsonNode` vs `ObjectNode` — read vs write:**

```
ObjectMapper (the engine)
    │
    ├── readTree(json)        → JsonNode   (read-only,  for parsing existing JSON)
    ├── readValue(json, Foo)  → Foo        (deserialize JSON → POJO)
    │
    ├── createObjectNode()    → ObjectNode (read-write, for building new JSON)
    └── writeValueAsString()  → String     (serialize object → JSON string)
```

`JsonNode` is the **read-only** base class — you can traverse and extract values but can't modify it. `ObjectNode` extends `JsonNode` and adds **write** methods (`.put()`, `.set()`). In the sink, we only need to read JSON that the deserializer already built, so `JsonNode` is sufficient.

**Why `readTree()` instead of `readValue(json, Map.class)`?** `readTree` preserves type information in `JsonNode` — you can ask `node.isInt()`, `node.isBoolean()`, etc. A `Map<String, Object>` loses this nuance (numbers might all become `Double`). The `extractValue()` method relies on `JsonNode.getNodeType()` to correctly map JSON types to Java types for the `PreparedStatement`.

**Essential JDBC concept — `information_schema` for runtime type discovery:**

```java
SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?
```

`information_schema` is a standard SQL metadata schema available in Postgres (and MySQL). It contains metadata about all tables, columns, types, constraints, etc. We query it once per table to learn the Postgres column types, then cache the result. This enables the `convertValue()` method to fix type mismatches (e.g. MySQL `TINYINT(1)` → Postgres `boolean`).

**Essential Java concept — intersection type cast for serialization:**

```java
this.primaryKeys = (Serializable & Map<String, List<String>>) primaryKeys;
```

`Map.of()` returns an unmodifiable map that IS serializable at runtime but doesn't declare `Serializable` in its type. This cast tells the Java compiler "this object is BOTH `Serializable` AND `Map`" — allowing Flink to serialize it when shipping the `Sink` to task managers. Without this cast, Flink would throw `NotSerializableException`.

**Essential concept — HikariCP connection pooling:**

```java
HikariConfig config = new HikariConfig();
config.setJdbcUrl(jdbcUrl);
config.setMaximumPoolSize(10);
this.dataSource = new HikariDataSource(config);

// Later, in flush():
Connection conn = dataSource.getConnection();   // borrows from pool (fast)
// ... use connection ...
conn.close();   // returns to pool (NOT actually closed)
```

Without pooling, every `getConnection()` creates a new TCP connection + authentication handshake (~5-20ms). With HikariCP, connections are pre-created and reused. `getConnection()` borrows one from the pool (~0.01ms), and `close()` returns it. This is critical for batch writes where you're getting connections every checkpoint interval.

---

#### 4. `Main.java` — The Pipeline Orchestrator

**Purpose:** Wires everything together — reads config, creates source/sink, defines the dataflow.

**Essential Flink concept — the pipeline structure:**

Every Flink job follows this pattern:

```java
// 1. Setup execution environment
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(3000);       // enable fault-tolerance

// 2. Define source
DataStream<CdcEvent> stream = env.fromSource(source, watermarkStrategy, "name");

// 3. Transform (optional)
stream.keyBy(CdcEvent::getTable)     // partition by table name

// 4. Define sink
      .sinkTo(new PGSinker(...));

// 5. Execute (nothing runs until this call!)
env.execute("Job Name");
```

**Critical:** `env.execute()` is what triggers the job. Everything before it just builds a DAG (directed acyclic graph) of operations. No data flows until `execute()` is called.

**Essential Flink concept — `keyBy()` before sink:**

```java
cdcStream
    .keyBy(CdcEvent::getTable)    // partition events by table name
    .sinkTo(new PGSinker(...));
```

`keyBy()` ensures all events for the same table go to the same subtask (parallel instance). This means:
- Events for `shipments` always go to the same `PostgresWriter` instance
- Events for `shipments_v0` might go to a different instance
- Within each writer, events arrive in CDC order (preserving consistency)

Without `keyBy()`, events from different tables could interleave unpredictably across writer instances.

**Essential Flink concept — `setParallelism(1)` for CDC source:**

```java
env.fromSource(mySQLSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
    .setParallelism(1);   // MUST be 1
```

MySQL binlog is a single sequential stream — there's only one binlog per MySQL server. Having multiple parallel readers would cause duplicate events or ordering issues. The parallelism is set to 1 for the source only; downstream operators (like the sink) can run with higher parallelism.

**Essential Flink concept — `WatermarkStrategy.noWatermarks()`:**

Watermarks are Flink's mechanism for handling event-time processing (e.g. "this event happened at 3:00 PM"). For CDC mirroring, we don't need event-time windows or late data handling — we just process events in arrival order. `noWatermarks()` tells Flink to skip watermark generation entirely.

**Essential Flink concept — checkpointing and exactly-once:**

```java
env.enableCheckpointing(3000);   // every 3 seconds
```

This creates a consistent snapshot of the entire pipeline state every 3 seconds:
- **Source:** saves the current binlog position
- **Sink:** `flush()` is called → all buffered events are committed to Postgres

If the job crashes, Flink restores from the last checkpoint: the source replays from the saved binlog position, and the sink re-receives (and re-upserts) the same events. Since upsert and delete are idempotent, the end result is correct.

---

#### Summary: What Each File Teaches You

| File | Flink Concepts | Java / Jackson Concepts | Database Concepts |
|------|---------------|------------------------|-------------------|
| `CdcEvent.java` | DataStream type requirements, Serializable | `Serializable`, `serialVersionUID` | CDC operation types (c/u/d/r) |
| `JsonCdcDeserializer.java` | `DebeziumDeserializationSchema`, `Collector`, `TypeInformation` | `transient`, lazy init, switch pattern matching (Java 21), `ObjectMapper`, `ObjectNode` (JSON writing) | Kafka Connect `Struct`, Debezium record structure |
| `PGSinker.java` | `Sink` / `SinkWriter` lifecycle, `write()` → `flush()` → `close()`, checkpoint-aligned batching | intersection types, `try-with-resources`, `ObjectMapper.readTree()`, `JsonNode` (JSON reading), `JsonNode` vs `ObjectNode` | JDBC transactions, `PreparedStatement`, `ON CONFLICT` upsert, `information_schema`, HikariCP pooling |
| `Main.java` | `StreamExecutionEnvironment`, `fromSource()`, `keyBy()`, `sinkTo()`, `execute()`, checkpointing, parallelism | `Properties`, classpath resource loading | — |

### Setup Steps

#### 1. Start Infrastructure

```bash
docker compose up -d
```

This starts MySQL, Kafka, Flink, and Postgres (`localhost:5432`, user: `postgres`, password: `postgres`, database: `pgdb`).

#### 2. Grant MySQL Replication Permissions

```bash
docker exec flink-self-traning-mysql-1 mysql -uroot -p123456 -e "
  GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'mysqluser'@'%';
  GRANT SELECT ON db_1.* TO 'mysqluser'@'%';
  FLUSH PRIVILEGES;
"
```

#### 3. Create Matching Tables in Postgres

The destination tables in Postgres **must exist before running the pipeline** and must have the same column names and a matching primary key. The sink uses `ON CONFLICT (pk)` for upsert, so the primary key is required.

```bash
docker exec -it postgres psql -U postgres -d pgdb -e "
  CREATE TABLE shipments (
    shipment_id INT PRIMARY KEY,
    order_id INT,
    origin VARCHAR(255),
    destination VARCHAR(255),
    is_arrived BOOLEAN
  );

  CREATE TABLE shipments_v0 (
    shipment_id INT PRIMARY KEY,
    order_id INT,
    origin VARCHAR(255),
    destination VARCHAR(255),
    is_arrived BOOLEAN
  );
"
```

**Important rules for Postgres destination tables:**

- Column names must match the MySQL source table column names exactly
- A primary key must be defined (used for `ON CONFLICT` upsert and `DELETE WHERE pk = ?`)
- Column types should be compatible (e.g. MySQL `INT` → Postgres `INT`, MySQL `VARCHAR` → Postgres `VARCHAR`)
- The table will **not** be auto-created — you must create it manually

#### 4. Configure application.properties

```properties
# MySQL CDC Source Configuration
mysql.hostname=localhost
mysql.port=3306
mysql.database=db_1
mysql.table=db_1.shipments,db_1.shipments_v0
mysql.username=mysqluser
mysql.password=mysqlpw

# Postgres Destination Configuration
postgres.hostname=localhost
postgres.port=5432
postgres.database=pgdb
postgres.username=postgres
postgres.password=postgres
```

#### 5. Configure Primary Keys in Main.java

The `PGSinker` needs to know which column(s) form the primary key for each table (used in the `ON CONFLICT` clause). This is configured in `Main.java`:

```java
Map<String, List<String>> primaryKeys = Map.of(
    "shipments",    List.of("shipment_id"),
    "shipments_v0", List.of("shipment_id")
);
```

For composite primary keys, list all columns:

```java
"order_items", List.of("order_id", "item_id")
```

#### 6. Run the Application

```bash
cd flinkConnectorsApp
../gradlew run
```

### Adding a New Table to Mirror

To add a new table (e.g. `orders`), you need to:

1. **Add the table to `mysql.table`** in `application.properties`:
   ```properties
   mysql.table=db_1.shipments,db_1.shipments_v0,db_1.orders
   ```

2. **Create the matching table in Postgres:**
   ```sql
   CREATE TABLE orders (
     order_id INT PRIMARY KEY,
     customer_name VARCHAR(255),
     total DECIMAL(10,2)
   );
   ```

3. **Add the primary key config** in `Main.java`:
   ```java
   Map<String, List<String>> primaryKeys = Map.of(
       "shipments",    List.of("shipment_id"),
       "shipments_v0", List.of("shipment_id"),
       "orders",       List.of("order_id")          // new
   );
   ```

No new deserializers, models, or sink classes are needed.

### Testing the Mirror Pipeline

After starting the application, make changes in MySQL and verify they appear in Postgres:

```bash
# Insert in MySQL
docker exec -it flink-self-traning-mysql-1 mysql -umysqluser -pmysqlpw db_1 -e "
  INSERT INTO shipments VALUES (1, 1, 'Shanghai', 'Tokyo', false);
"

# Verify in Postgres
docker exec -it postgres psql -U postgres -d pgdb -c "SELECT * FROM shipments;"

# Update in MySQL
docker exec -it flink-self-traning-mysql-1 mysql -umysqluser -pmysqlpw db_1 -e "
  UPDATE shipments SET is_arrived = true WHERE shipment_id = 1;
"

# Verify update in Postgres
docker exec -it postgres psql -U postgres -d pgdb -c "SELECT * FROM shipments;"

# Delete in MySQL
docker exec -it flink-self-traning-mysql-1 mysql -umysqluser -pmysqlpw db_1 -e "
  DELETE FROM shipments WHERE shipment_id = 1;
"

# Verify delete in Postgres
docker exec -it postgres psql -U postgres -d pgdb -c "SELECT * FROM shipments;"
```

### Known Limitations

- **No auto table creation** — Postgres destination tables must be created manually before running the pipeline
- **No schema evolution** — if a column is added/dropped in MySQL, the Postgres table must be updated manually or the sink will fail
- **Column name matching** — MySQL and Postgres column names must be identical (case-sensitive)

### Troubleshooting (Mirror Pipeline)

#### Boolean Column Type Mismatch

**Error message:**
```
PSQLException: ERROR: column "is_arrived" is of type boolean but expression is of type double precision
```

**Cause:** MySQL CDC sends `BOOLEAN`/`TINYINT(1)` columns as `Short` (0/1), which gets serialized as a JSON number (e.g. `"is_arrived": 0`). When the sink reads this back from JSON, Jackson treats it as a number. Postgres then rejects it because the target column expects a `boolean`, not a `double`.

**How it's solved:** The `PGSinker` queries Postgres `information_schema.columns` on the first event per table to discover the target column types. When a column is `boolean` in Postgres but the incoming value is a number, the sink's `convertValue()` method automatically converts it (`0 → false`, non-zero → `true`). This metadata is cached so the query only runs once per table.

**If you hit a similar type mismatch for other column types**, add a new conversion case in `PostgresWriter.convertValue()` in `PGSinker.java`:

```java
private Object convertValue(Object val, String pgType) {
    if (val == null || pgType == null) {
        return val;
    }
    // Boolean: MySQL CDC sends TINYINT(1) as Short (0/1)
    if ("boolean".equalsIgnoreCase(pgType) || "bool".equalsIgnoreCase(pgType)) {
        if (val instanceof Number n) {
            return n.intValue() != 0;
        }
    }
    // Add more conversions here as needed, e.g.:
    // if ("timestamp".equalsIgnoreCase(pgType) && val instanceof Long l) { ... }
    return val;
}
```