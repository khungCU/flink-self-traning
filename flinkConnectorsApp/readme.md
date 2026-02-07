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