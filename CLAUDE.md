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
