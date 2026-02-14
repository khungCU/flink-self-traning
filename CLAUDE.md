# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Apache Flink training project with multiple sub-projects demonstrating various Flink streaming patterns and custom source implementations. The repository uses a Gradle multi-project build structure with three main modules: `app`, `eventdriven`, and `models`.

## Build System

This project uses Gradle 9.0.0 with Java 21 toolchain.

### Common Build Commands

```bash
# Build entire project
./gradlew build

# Build specific sub-project
./gradlew :app:build
./gradlew :eventdriven:build
./gradlew :models:build

# Run tests
./gradlew test

# Run tests for specific sub-project
./gradlew :app:test

# Clean build
./gradlew clean build

# Build Kafka producer fat JAR
./gradlew producerJar
```

### Running Applications

The `app` and `eventdriven` sub-projects each have their own main classes configured:

```bash
# Run app sub-project (default main: EnrichmentSimpleApp)
./gradlew :app:run

# Run eventdriven sub-project (Slack source)
./gradlew :eventdriven:run

# Run with environment variables from .env file (automatically loaded by build.gradle)
./gradlew :app:run
```

To run a specific main class, modify the `mainClass` in the respective `build.gradle` file.

## Project Structure

The codebase is organized as a Gradle multi-project:

- **`app/`** - Main Flink applications demonstrating various streaming patterns
  - `statefulTransformation/` - Join operations, windowing, grouping examples
  - `statelessTransformation/` - Basic transformations and filters
  - `asyncLookup/` - Asynchronous enrichment patterns
  - `kafkaClient/` - Kafka producer/consumer implementations
  - `customDataGen/` - Custom data generator sources
  - `utils/` - Shared utilities

- **`eventdriven/`** - Custom Slack source connector implementation
  - `SlackSource/` - Complete Flink Source API implementation for Slack events
  - `App/` - Main application using the Slack source

- **`models/`** - Shared data models (Java library project)
  - Event, User, Client, EventUserEnrichment POJOs

## flinkConnectorsApp — CDC Mirror Pipeline

A standalone sub-project (not part of the Gradle multi-project) that mirrors MySQL tables to Postgres/Snowflake in real-time using Flink CDC.

### Build & Run

```bash
cd flinkConnectorsApp
../gradlew run          # runs Main.java (requires Docker services + .env)
../gradlew build        # compile only
```

Requires Docker services running: MySQL (`localhost:3306`), Postgres (`localhost:5432`). Config in `src/main/resources/application.properties`. Environment variables loaded from root `.env` file.

### Architecture & Data Flow

```
MySQL (binlog) → MySqlSource → JsonCdcDeserializer → CdcEvent → keyBy(table) → PGSinker/SFSinker → Postgres/Snowflake
```

### Source Files

| File | Purpose |
|------|---------|
| `Main.java` | Pipeline entry point. Configures MySqlSource, checkpointing (3s), keyBy(table), sinkTo(PGSinker) |
| `MainV0.java` | Earlier version (single-table POJO approach, kept for reference) |
| `deserializer/JsonCdcDeserializer.java` | Implements `DebeziumDeserializationSchema<CdcEvent>`. Extracts table name, op type, before/after JSON from Debezium `SourceRecord`. Uses Jackson `ObjectMapper` + `ObjectNode` to build JSON strings |
| `deserializer/ShipmentDebeziumDeserializer.java` | Single-table POJO deserializer (earlier approach, kept for comparison) |
| `model/CdcEvent.java` | Generic CDC event POJO: `table`, `op` (c/r/u/d), `before` (JSON string), `after` (JSON string) |
| `model/Shipment.java` | Single-table POJO (used by V0 approach) |
| `model/ShipmentCdcEvent.java` | Single-table CDC event (used by V0 approach) |
| `sinker/PGSinker.java` | Postgres sink using `INSERT ... ON CONFLICT DO UPDATE`. Batched flush with JDBC transactions. Uses `getColumnTypes()` for boolean conversion |
| `sinker/SFSinker.java` | Snowflake sink using `MERGE INTO ... USING`. Same batching pattern as PGSinker |

### Key Design Decisions

- **Multi-table generic approach**: `CdcEvent` carries table name + raw JSON, so one pipeline handles all tables without per-table POJOs
- **keyBy(CdcEvent::getTable)**: Guarantees ordering per table. All events for a table go to the same subtask
- **Batched flush**: `write()` buffers events, `flush()` executes all in a single JDBC transaction at checkpoint boundaries. Trade-off: up to checkpoint-interval latency for consistency
- **getColumnTypes()**: Queries `information_schema.columns` once per table per writer to fix MySQL `TINYINT(1)` → boolean mismatch. Cached in plain `HashMap` (not Flink state)
- **HikariCP connection pool**: Each `SinkWriter` instance has its own pool (max 10 connections)
- **Serializable cast trick**: `(Serializable & Map<...>) primaryKeys` — makes the Map serializable for Flink's serialization without creating a new class

### Dependencies (beyond standard Flink)

| Dependency | Purpose |
|------------|---------|
| `flink-connector-mysql-cdc:3.2.1` | Debezium-based MySQL CDC source (transitively brings `kafka-connect-api`) |
| `HikariCP:5.1.0` | JDBC connection pooling |
| `postgresql:42.7.1` | Postgres JDBC driver |
| `snowflake-jdbc:3.16.1` | Snowflake JDBC driver |
| Jackson (transitive) | JSON processing via `ObjectMapper`, `JsonNode`, `ObjectNode` |

### Lesson Docs (in `docs/`)

Educational markdown files created alongside this module:

| Doc | Covers |
|-----|--------|
| `debezium-deserializer-lesson.md` | `DebeziumDeserializationSchema`, Struct, single-table vs multi-table approaches |
| `jackson-objectmapper-lesson.md` | ObjectMapper, ObjectNode vs JsonNode, tree model vs POJO binding |
| `kafka-connect-data-lesson.md` | Struct, Field, SourceRecord, transitive dependency chain |
| `flink-sink-lesson.md` | Sink2 API, SinkWriter lifecycle, sinkTo after map vs keyBy |
| `java-sql-lesson.md` | Connection, PreparedStatement, ResultSet, transactions, dynamic SQL |
| `hikari-quick-lesson.md` | HikariCP connection pooling |
| `get-column-types-lesson.md` | information_schema queries, caching, boolean conversion |
| `schema-conversion-tools-lesson.md` | AWS SCT, SnowConvert for DDL migration |

## Key Architecture Patterns

### Multi-Project Dependencies

The `models` sub-project is a shared library used by both `app` and `eventdriven`. Changes to models require rebuilding dependent projects:

```bash
# After modifying models
./gradlew :models:build :app:build :eventdriven:build
```

### Java Module System Compatibility

**CRITICAL**: All Flink applications require JVM module system flags for Java 9+ compatibility. These are already configured in:
- `build.gradle` files for each sub-project (`run` task, `test` task, `application` block)
- `.vscode/launch.json` for IDE debugging

Required JVM args:
```
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
```

When creating new run configurations or debugging in IDE, always include these flags.

### Custom Flink Source Implementation

The `eventdriven` module demonstrates a complete Flink Source API implementation (`SlackSource`):
- `SlackSource` - Main source implementation (Source interface)
- `SlackSourceReader` - Reads events from Slack socket connection
- `SlackSplitEnumerator` - Manages split assignment
- `SlackSplit` - Represents a Slack channel split
- Serializers for splits and checkpoint state

This pattern can be adapted for other event-driven sources.

## Docker Environment

The project includes a `compose.yaml` with development infrastructure:

```bash
# Start all services (Kafka, Schema Registry, Flink cluster, MySQL)
docker compose up -d

# Stop services
docker compose down

# View logs
docker compose logs -f [service-name]
```

Services:
- **Kafka broker**: `localhost:9092`
- **Schema Registry**: `localhost:8081`
- **Kafka REST Proxy**: `localhost:8082`
- **Flink JobManager UI**: `localhost:9081`
- **MySQL**: `localhost:3306` (user: mysqluser, password: mysqlpw, db: db_1)

The `events.topic` Kafka topic is automatically created on startup.

## Environment Configuration

Create a `.env` file in the project root for environment-specific configuration (e.g., Slack tokens, API keys). The Gradle `run` task automatically loads variables from `.env`.

Example `.env`:
```
SLACK_BOT_TOKEN=xoxb-...
SLACK_APP_TOKEN=xapp-...
SLACK_CHANNEL_ID=C...
```

## Flink Version

This project uses **Flink 1.18.0**. When adding new Flink dependencies or connectors, ensure version compatibility.

## Testing

The project uses JUnit Jupiter for testing. Flink test utilities are available:

```bash
# Run all tests
./gradlew test

# Run test with output
./gradlew test --info

# Run specific test class
./gradlew :app:test --tests "flink.self.traning.TestSimpleHarness"
```

Test harness example location: `app/src/test/java/flink/self/traning/TestSimpleHarness.java`

## Common Development Patterns

### Stateful Transformations

See `app/src/main/java/flink/self/training/statefulTransformation/` for examples of:
- Stream joins (simple, windowed, flat join)
- Grouping and aggregation
- Watermark and event time processing
- Window operations

### Enrichment Patterns

Multiple enrichment approaches demonstrated:
- Simple join (memory-based)
- Windowed join
- Async lookup join with external services

### Data Model Location

All shared POJOs are in `models/src/main/java/com/flink/self/training/`. When creating new data models that will be used across multiple sub-projects, add them to the `models` module.
