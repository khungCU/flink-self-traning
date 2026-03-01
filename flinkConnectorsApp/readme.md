# flinkConnectorsApp — CDC Mirror Pipeline

Real-time MySQL-to-Postgres replication using Flink CDC. Captures MySQL binlog events,
normalizes them into typed POJOs, and sinks to Postgres via upsert/delete.

## Architecture Overview

```
MySQL binlog
    |
    v
MySqlSource (Flink CDC connector)
    |
    v
JsonCdcDeserializer            -- SourceRecord -> CdcEvent (generic, table-agnostic)
    |
    v
CdcEvent stream                -- keyBy(table) ensures per-table ordering
    |
    v
SchemaNormalizer               -- JSON -> typed POJO via Jackson, routed to side outputs
    |
    +--> [shipments]    side output  --+
    +--> [shipments_v0] side output  --+--> union --> PGSinker --> Postgres
    +--> [schema-drift] side output  ----> print (alerting)
```

## Class Roles

### Entry Point

| Class | Role |
|-------|------|
| `Main.java` | Infrastructure setup only. Loads `application.properties`, creates `StreamExecutionEnvironment` with checkpointing (3s), builds `MySqlSource`, `TableRegistry`, and `PGSinker`, then delegates all pipeline assembly to `DBSyncWithSchemaWorkflow.Builder`. |
| `MainV0.java` | Earlier single-table version kept for reference. |

### Workflows

| Class | Role |
|-------|------|
| `DBSyncWithSchemaWorkflow` | Pipeline assembly for CDC sync. Accepts a pluggable `DataStream<CdcEvent>` source, `TableRegistry`, and `Sink<MessageNormalized>`. Supports two configuration styles: `Builder` (validated at `build()`) and direct construction with chained `setSource / setRegistry / setSink` setters (validated at `execute()`). |

### Deserializer

| Class | Role |
|-------|------|
| `JsonCdcDeserializer` | Converts Debezium `SourceRecord` into `CdcEvent`. Extracts op type, table name, and before/after states as raw JSON strings. Table-agnostic — does not need to know individual table schemas. |
| `ShipmentDebeziumDeserializer` | Earlier single-table deserializer (kept for comparison). |

### Models

| Class | Role |
|-------|------|
| `CdcEvent` | Generic CDC event container. Holds `table`, `op` (c/r/u/d), `before` (JSON string), `after` (JSON string). Flows from deserializer to SchemaNormalizer. |
| `MessageNormalized` | Interface that all table POJOs implement. Defines `getOp/setOp` and `getTable/setTable` metadata accessors. |
| `Shipment` | Typed POJO for the `shipments` table (shipmentId, orderId, origin, destination, isArrived). |
| `ShipmentV0` | Typed POJO for the `shipments_v0` table. |
| `Unknown` | Fallback POJO for unregistered tables. Stores raw JSON instead of typed fields. Emitted to the schema-drift side output for alerting. |

### Reconciliation

| Class | Role |
|-------|------|
| `TableRegistry` | **Single source of truth** for all per-table config: POJO class, primary keys, and Flink `OutputTag`. Also produces the `MySqlSource`-compatible `tableList` string via `toMySqlTableList(database)`, so Debezium capture and Flink pipeline handling are always in sync. Adding a new table = one `register()` call here. No other files need editing. |
| `SchemaNormalizer` | Flink `KeyedProcessFunction` that deserializes `CdcEvent` JSON into typed POJOs using Jackson. Routes each event to the correct side output based on `TableRegistry`. Unknown tables go to the schema-drift output. |

### Sinkers

| Class | Role |
|-------|------|
| `PGSinker` | Postgres sink (`Sink<MessageNormalized>`). Buffers events in `write()`, flushes all in a single JDBC transaction at each Flink checkpoint. Uses reflection to extract POJO fields, builds `INSERT ... ON CONFLICT DO UPDATE` (upsert) or `DELETE` SQL dynamically. |
| `SFSinker` | Snowflake sink. Same batching pattern as PGSinker using `MERGE INTO`. |

## How to Add a New Table

Only **one file** needs editing: `TableRegistry.java`.

### Step 1: Create the POJO

Add a new class in `model/` that implements `MessageNormalized`:

```java
package model;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Order implements MessageNormalized {
    // Metadata (not written to Postgres)
    @JsonIgnore private String op;
    @JsonIgnore private String table;

    // Database columns (camelCase — Jackson converts from snake_case automatically)
    private int orderId;
    private String customerName;
    private double totalAmount;

    // Getters and setters for all fields...
    @Override public String getOp() { return op; }
    @Override public void setOp(String op) { this.op = op; }
    @Override public String getTable() { return table; }
    @Override public void setTable(String table) { this.table = table; }
    // ... column getters/setters
}
```

**Naming convention**: Java field names use camelCase (e.g. `orderId`). Jackson with
`SNAKE_CASE` naming strategy automatically maps from/to `order_id` in JSON and Postgres.

### Step 2: Register in TableRegistry

Add one line to the `TableRegistry` constructor:

```java
public TableRegistry() {
    register("shipments",    Shipment.class,   List.of("shipment_id"));
    register("shipments_v0", ShipmentV0.class,  List.of("shipment_id"));
    register("orders",       Order.class,       List.of("order_id"));  // <-- new
}
```

Parameters:
- **Table name** — must match the MySQL table name exactly
- **POJO class** — the `MessageNormalized` implementation from step 1
- **Primary keys** — snake_case column names used for Postgres `ON CONFLICT` and `DELETE WHERE`

That's it. No changes needed in `Main.java`, `SchemaNormalizer.java`, `PGSinker.java`, or `application.properties`.

### What happens automatically

- `SchemaNormalizer` picks up the new POJO class and deserializes CDC JSON into it
- A new Flink side output is created and routed automatically
- `DBSyncWithSchemaWorkflow` unions the new side output into the Postgres-bound stream
- `PGSinker` uses the registered primary keys for upsert/delete SQL
- `toMySqlTableList()` includes the new table in `MySqlSource.tableList` automatically
- Extra columns in MySQL that don't exist in the POJO are silently dropped (Jackson `FAIL_ON_UNKNOWN_PROPERTIES=false`)
- Missing columns default to null

### Prerequisites

- The target Postgres table must already exist with matching column names (snake_case)

## Key Design Decisions

- **`keyBy(CdcEvent::getTable)`** — all events for a table go to the same Flink subtask, preserving per-table ordering
- **Batched flush at checkpoints** — `write()` buffers, `flush()` commits all events in one JDBC transaction. Trade-off: Postgres lags behind MySQL by up to the checkpoint interval (3s)
- **Reflection-based SQL** — PGSinker uses Java reflection on the POJO to extract field names/values, so adding new columns to a POJO automatically flows through to SQL
- **Type conversion via POJO** — Jackson coerces MySQL types into the correct Java types when deserializing into typed POJOs (e.g. `0/1` -> `Boolean`), so PGSinker doesn't need to query Postgres `information_schema` for column types
- **Schema drift detection** — unregistered tables emit to a `schema-drift` side output for monitoring/alerting
- **`TableRegistry` as single source of truth** — `toMySqlTableList(database)` derives the CDC capture list directly from registered tables, so `MySqlSource` and `TableRegistry` are always in sync without manual `application.properties` maintenance
- **Builder pattern for pipeline** — `DBSyncWithSchemaWorkflow` decouples pipeline assembly from infrastructure. Source and sink are pluggable: swap `MySqlSource` for `env.fromElements(...)` and `PGSinker` for a test sink without touching the workflow logic

## Design Evolution: V0 vs Current

This project went through two design iterations. Both are preserved in the codebase — V0 files are kept for comparison.

### V0: Single-Table, Typed CDC Events (`MainV0.java`)

```
MySQL binlog
    |
    v
ShipmentDebeziumDeserializer     -- Struct -> ShipmentCdcEvent (hardcoded to shipments)
    |                                  |
    |                           structToShipment()  -- manual field-by-field extraction
    |                                  |
    v                                  v
ShipmentCdcEvent                 before: Shipment (typed POJO)
  (typed, single-table)          after:  Shipment (typed POJO)
    |
    v
print()                          -- no sink to Postgres in V0
```

**Files involved:**
- `ShipmentDebeziumDeserializer` — hardcoded deserializer with `structToShipment()` that manually maps each column: `struct.getInt32("shipment_id")`, `struct.getString("origin")`, etc.
- `ShipmentCdcEvent` — typed CDC wrapper where `before`/`after` are `Shipment` POJOs (not JSON strings)
- `Shipment` — the POJO

**Key characteristics:**
- Deserializer is **coupled to one table** — `structToShipment()` references column names directly
- Before/after are **typed POJOs** — type-safe but inflexible
- Boolean conversion is **manual** — explicit `instanceof Short` / `instanceof Integer` checks in deserializer
- Adding a new table requires: new POJO + new CdcEvent wrapper + new deserializer + new pipeline wiring

### Current: Multi-Table, Generic CDC Events with Reconciliation (`Main.java`)

```
MySQL binlog
    |
    v
JsonCdcDeserializer              -- Struct -> CdcEvent (any table, JSON strings)
    |                                  |
    |                           structToJson()  -- schema-driven iteration, no hardcoded columns
    |                                  |
    v                                  v
CdcEvent                         before: String (raw JSON)
  (generic, any table)           after:  String (raw JSON)
    |
    v  keyBy(table)
    |
    v
SchemaNormalizer                 -- JSON -> typed POJO via Jackson + TableRegistry
    |                                  |
    |                           readValue(json, Shipment.class)  -- Jackson handles type coercion
    |                                  |
    v                                  v
MessageNormalized                side outputs per table
  (Shipment, ShipmentV0, etc.)       |
    |                                  |
    v  union                           v
    |                           Unknown -> schema-drift alerting
    v
PGSinker                        -- reflection-based SQL, any table
```

**Key characteristics:**
- Deserializer is **table-agnostic** — iterates over `Struct.schema().fields()` dynamically
- Before/after are **JSON strings** — decoupled from any specific table schema
- Type coercion is **automatic** — Jackson handles `0/1 -> Boolean` when deserializing into typed POJOs
- Adding a new table requires: new POJO + one `register()` call in `TableRegistry`

### Side-by-Side Comparison

| Aspect | V0 (Single-Table) | Current (Multi-Table + Reconciliation) |
|--------|-------------------|---------------------------------------|
| **Deserializer** | `ShipmentDebeziumDeserializer` — hardcoded column names (`struct.getInt32("shipment_id")`) | `JsonCdcDeserializer` — schema-driven iteration, no column names |
| **CDC event type** | `ShipmentCdcEvent` with typed `Shipment before/after` | `CdcEvent` with `String before/after` (raw JSON) |
| **Type conversion** | Manual in deserializer (`instanceof Short`, `instanceof Integer` → boolean) | Automatic by Jackson (`0/1` → `Boolean isArrived`) + POJO field types |
| **Adding a table** | New POJO + new `XxxCdcEvent` + new `XxxDeserializer` + pipeline wiring in Main | New POJO + one `register()` line in `TableRegistry` |
| **Files to edit** | 4+ files | 2 files (POJO + TableRegistry) |
| **Schema drift** | Unknown tables silently ignored or crash | Routed to `schema-drift` side output for alerting |
| **Sink type info** | N/A (V0 had no sink) | PGSinker uses reflection — no per-table SQL code |
| **Boolean handling** | Deserializer manually checks `Short`/`Integer`/`Boolean` | Jackson coerces to POJO field type; JDBC maps natively |
| **Trade-off** | Compile-time safety — all column access is typed | Runtime flexibility — columns discovered via JSON keys + reflection |

### Why the Design Changed

The V0 approach works well for **one or two tables** — you get compile-time type safety and clear, readable code. But it doesn't scale:

```
V0: Adding 10 tables = 10 POJOs + 10 CdcEvent wrappers + 10 deserializers + 10 pipeline branches
Current: Adding 10 tables = 10 POJOs + 10 register() lines in one file
```

The current design pushes table-specific knowledge to **two places only**:
1. **POJO class** — defines which columns to keep (schema normalization)
2. **`TableRegistry.register()`** — connects table name, POJO class, and primary keys

Everything else — deserialization, routing, SQL generation, type conversion — is generic and table-agnostic.

### What V0 Does Better

V0 isn't strictly worse — it has advantages for certain use cases:

- **Compile-time column access** — `event.getAfter().getShipmentId()` catches typos at compile time; JSON-based access discovers errors at runtime
- **IDE support** — autocomplete works on typed POJOs; JSON string fields have no autocomplete
- **Simpler mental model** — no Jackson, no reflection, no side outputs. The data flow is straightforward: Struct → POJO → done
- **Better for single-table use cases** — if you're only mirroring one table, V0 has less moving parts

The current design trades these for **scalability and maintainability** when handling many tables.

## Build & Run

```bash
cd flinkConnectorsApp
../gradlew build        # compile
../gradlew run          # run (requires Docker services + .env)
```

Requires Docker services: MySQL (`localhost:3306`), Postgres (`localhost:5432`).
Config in `src/main/resources/application.properties`.

## Project Structure

```
flinkConnectorsApp/src/main/java/
├── Main.java                              # Infrastructure setup + workflow wiring
├── MainV0.java                            # Earlier single-table version (reference)
├── workflows/
│   └── DBSyncWithSchemaWorkflow.java      # Pipeline assembly (Builder + fluent setters)
├── model/
│   ├── CdcEvent.java                      # Generic CDC event (JSON-based, any table)
│   ├── MessageNormalized.java             # Interface for all table POJOs
│   ├── Shipment.java                      # POJO: shipments table
│   ├── ShipmentV0.java                    # POJO: shipments_v0 table
│   ├── Unknown.java                       # Fallback POJO for unregistered tables
│   └── ShipmentCdcEvent.java             # Legacy single-table CDC event
├── deserializer/
│   ├── JsonCdcDeserializer.java           # Debezium -> CdcEvent (table-agnostic)
│   └── ShipmentDebeziumDeserializer.java  # Legacy single-table deserializer
├── reconciliation/
│   ├── TableRegistry.java                 # Single source of truth for table config + MySqlSource tableList
│   └── SchemaNormalizer.java              # JSON -> typed POJO + side output routing
└── sinker/
    ├── PGSinker.java                      # Postgres sink (upsert/delete, batched)
    └── SFSinker.java                      # Snowflake sink (merge, batched)
```

## Prerequisites

- Java 21
- Gradle
- Docker & Docker Compose

## Quick Start

### 1. Start Infrastructure

```bash
docker compose up -d
```

This starts:
- MySQL (port 3306) with initial schema from `mysql-init/db1.sql`
- Kafka broker (port 9092)
- Flink JobManager & TaskManager
- Postgres (port 5432)

### 2. Grant MySQL Replication Permissions

```bash
docker exec flink-self-traning-mysql-1 mysql -uroot -p123456 -e "
  GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'mysqluser'@'%';
  GRANT SELECT ON db_1.* TO 'mysqluser'@'%';
  FLUSH PRIVILEGES;
```

### 3. Create Matching Tables in Postgres

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

### 4. Configure application.properties

```properties
mysql.hostname=localhost
mysql.port=3306
mysql.database=db_1
# mysql.table is no longer needed — derived automatically from TableRegistry.toMySqlTableList()
mysql.username=mysqluser
mysql.password=mysqlpw

postgres.hostname=localhost
postgres.port=5432
postgres.database=pgdb
postgres.username=postgres
postgres.password=postgres
```

### 5. Run the Application

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
| `serverId` | Unique server ID range for this connector | `7100-7104` |
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

### Startup Options

| Startup Option | Description | Use Case |
|----------------|-------------|----------|
| `StartupOptions.initial()` | **(Default)** Full snapshot then binlog streaming | First-time setup |
| `StartupOptions.latest()` | Skip snapshot, start from latest binlog position | Only new changes needed |
| `StartupOptions.earliest()` | Skip snapshot, start from earliest available binlog | Replay all history |
| `StartupOptions.specificOffset(file, pos)` | Start from specific binlog position | Recovery scenarios |
| `StartupOptions.timestamp(timestamp)` | Start from events after a timestamp | Time-based recovery |

## Testing the Pipeline

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

# Delete in MySQL
docker exec -it flink-self-traning-mysql-1 mysql -umysqluser -pmysqlpw db_1 -e "
  DELETE FROM shipments WHERE shipment_id = 1;
"
```

## Dependencies

| Dependency | Purpose |
|------------|---------|
| `flink-connector-mysql-cdc:3.2.1` | Debezium-based MySQL CDC source |
| `HikariCP:5.1.0` | JDBC connection pooling |
| `postgresql:42.7.1` | Postgres JDBC driver |
| `snowflake-jdbc:3.16.1` | Snowflake JDBC driver |
| Jackson (transitive) | JSON processing via ObjectMapper |

## Troubleshooting

### Java Module Access Error

Add these JVM args:
```
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
```

### Server ID Conflict Error

```
A slave with the same server_uuid/server_id as this slave has connected to the master
```

Change `serverId` to a unique range, or kill lingering binlog connections:
```bash
docker exec flink-self-traning-mysql-1 mysql -uroot -p123456 -e "SHOW PROCESSLIST;" | grep "Binlog"
docker exec flink-self-traning-mysql-1 mysql -uroot -p123456 -e "KILL <id>;"
```

### Boolean Column Type Mismatch

```
PSQLException: ERROR: column "is_arrived" is of type boolean but expression is of type double precision
```

This is handled automatically by the reconciliation layer. Jackson coerces MySQL `TINYINT(1)` (sent as `0`/`1` in JSON) into `Boolean` when deserializing into typed POJOs (e.g. `Shipment.isArrived`). By the time PGSinker receives the POJO, the value is already a Java `Boolean` — JDBC maps it to Postgres `boolean` natively.

### Binlog Not Available

Use `StartupOptions.latest()` or `StartupOptions.initial()` to start from a fresh position.

## Known Limitations

- **No auto table creation** — Postgres tables must be created manually
- **No schema evolution** — column additions/drops in MySQL require manual Postgres DDL changes
- **Column name matching** — MySQL and Postgres column names must be identical (case-sensitive)
